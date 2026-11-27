//! Volume directories of a pool.
//!
//! ```text
//! <pool>/volumes/<volume>/data         the volume: mounted into the pod, carries the project and the limit
//! <pool>/volumes/<volume>/volume.json  passport: what the volume is, kept next to the data
//! <pool>/trash/<volume>.<expiry>/      a removed volume kept until `expiry` (Unix seconds)
//! <pool>/incoming/<volume>/            a copy of a volume that is moving to this node
//! ```
//!
//! The directory is the state: its project id is read from it, its limit from the quota.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::config::Pool;
use crate::quota::{self, Limits};

const VOLUMES: &str = "volumes";
const TRASH: &str = "trash";
const INCOMING: &str = "incoming";
const DATA: &str = "data";
const PASSPORT: &str = "volume.json";

/// A volume name becomes a directory name: it must not escape the pool.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\0']) && name.len() <= 253
}

fn volume_dir(pool: &Pool, volume: &str) -> PathBuf {
    pool.path.join(VOLUMES).join(volume)
}

/// Directory mounted into the pod.
pub fn data_dir(pool: &Pool, volume: &str) -> PathBuf {
    volume_dir(pool, volume).join(DATA)
}

pub fn exists(pool: &Pool, volume: &str) -> bool {
    data_dir(pool, volume).is_dir()
}

/// What a volume directory is created with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Params<'a> {
    pub limits: Limits,
    /// Permissions of the data directory.
    pub mode: u32,
    /// Passport of the volume, a JSON document.
    pub passport: &'a str,
}

fn subdirectories(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut directories = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            directories.push(entry.path());
        }
    }
    directories.sort();
    Ok(directories)
}

/// Project ids of all volume directories of `pools`, the ones in the trash included: they are
/// still charged for their data.
fn used_projects(pools: &[Pool]) -> io::Result<BTreeSet<u32>> {
    let mut used = BTreeSet::new();
    for pool in pools.iter().filter(|p| p.quota) {
        for parent in [VOLUMES, TRASH, INCOMING] {
            for directory in subdirectories(&pool.path.join(parent))? {
                match quota::project(&directory.join(DATA)) {
                    Ok(project) => {
                        used.insert(project.id);
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
        }
    }
    Ok(used)
}

/// The lowest free project id at or above `base`. Ids are unique across all pools of the node:
/// two pools may share a filesystem.
fn free_project(pools: &[Pool], base: u32) -> io::Result<u32> {
    let used = used_projects(pools)?;
    (base.max(1)..=u32::MAX).find(|id| !used.contains(id)).ok_or_else(|| io::Error::other("no free project id"))
}

/// Held while a project id is picked and while a directory moves against the order ids are
/// looked for in. One agent runs on a node, so a lock of the process is a lock of the node.
static PROJECTS: Mutex<()> = Mutex::new(());

fn lock_projects() -> MutexGuard<'static, ()> {
    // The lock guards no data that a panic could have left half-written.
    PROJECTS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The project of the data directory `data`; a directory without one gets the lowest free id.
///
/// Looking for a free id and taking it is one step: the main loop of the agent creates volumes
/// while its receiving tasks prepare copies, and two directories that found the same id free
/// would share their accounting and their limit.
fn assign_project(pools: &[Pool], base: u32, data: &Path) -> io::Result<u32> {
    let _picking = lock_projects();
    let current = quota::project(data)?.id;
    if current != 0 {
        return Ok(current);
    }
    let project = free_project(pools, base)?;
    quota::set_project(data, project)?;
    Ok(project)
}

/// Writes the passport next to the data, unless it is there as it has to be. A passport is
/// either the old one or the new one, also after a loss of power: the new one is on the disk
/// before it takes the name, and the directory is flushed after. A rename that survived without
/// the contents of its file would leave an empty passport - on a sealed copy, a volume that
/// cannot be told what it is.
fn write_passport(dir: &Path, passport: &str) -> io::Result<()> {
    use std::io::Write;
    let path = dir.join(PASSPORT);
    if fs::read_to_string(&path).is_ok_and(|current| current == passport) {
        return Ok(());
    }
    let temporary = dir.join(format!(".{PASSPORT}.tmp"));
    let mut file = fs::File::create(&temporary)?;
    file.write_all(passport.as_bytes())?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    fs::File::open(dir)?.sync_all()
}

/// Creates the volume directory if it is missing and brings its limits and passport in line with
/// `params`. Safe to repeat. `pools` and `project_id_base` are where a new project id comes from.
pub fn ensure(pools: &[Pool], project_id_base: u32, pool: &Pool, volume: &str, params: &Params) -> io::Result<()> {
    let dir = volume_dir(pool, volume);
    fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
    let data = dir.join(DATA);
    if !data.is_dir() {
        fs::create_dir(&data)?;
        // create_dir applies the umask; the mode is part of the contract with the pod.
        fs::set_permissions(&data, fs::Permissions::from_mode(params.mode))?;
    }
    if pool.quota {
        let project = assign_project(pools, project_id_base, &data)?;
        // The agent comes here on every pass, for every volume of the node. Setting a limit is
        // a change of the filesystem, reading it is not: it is set when it is not what it has
        // to be. A filesystem that rounds a limit to its own blocks gets it set every time.
        if quota::limits(&pool.path, project)? != params.limits.as_kept() {
            quota::set_limits(&pool.path, project, params.limits)?;
        }
    }
    write_passport(&dir, params.passport)
}

/// How long a removal waits for the filesystem to release the space.
const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// Removes a volume directory (`<...>/<volume>` with `data` inside) and the limits of its project.
///
/// Returns only when the project is charged nothing: XFS frees the blocks of removed files in
/// the background, and a volume that got the same project id meanwhile would start with the
/// usage of the removed one.
fn remove_directory(pool: &Pool, dir: &Path) -> io::Result<()> {
    let project = match quota::project(&dir.join(DATA)) {
        Ok(project) if pool.quota => Some(project.id).filter(|id| *id != 0),
        _ => None,
    };
    match fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let Some(project) = project else { return Ok(()) };
    let deadline = Instant::now() + RELEASE_TIMEOUT;
    loop {
        let usage = quota::usage(&pool.path, project)?;
        if usage.used_bytes == 0 && usage.used_inodes == 0 {
            break;
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "project {project} of the removed volume is still charged {} bytes",
                usage.used_bytes
            )));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    // A limit left behind would apply to the next volume that gets this project id.
    quota::set_limits(&pool.path, project, Limits::default())
}

/// Removes the volume and its data. Safe to repeat.
pub fn remove(pool: &Pool, volume: &str) -> io::Result<()> {
    remove_directory(pool, &volume_dir(pool, volume))
}

/// Moves the volume to the trash of its pool, where it stays until `expires` (Unix seconds).
/// The data keeps its project and stays charged to it. Safe to repeat: a volume that is not
/// there has been moved already.
pub fn trash(pool: &Pool, volume: &str, expires: u64) -> io::Result<()> {
    let from = volume_dir(pool, volume);
    if !from.exists() {
        return Ok(());
    }
    let trash = pool.path.join(TRASH);
    fs::DirBuilder::new().recursive(true).mode(0o700).create(&trash)?;
    // Two removals of volumes with one name may fall into the same second.
    let mut expires = expires;
    loop {
        let to = trash.join(format!("{volume}.{expires}"));
        if !to.exists() {
            return fs::rename(&from, to);
        }
        expires += 1;
    }
}

/// A removed volume kept in the trash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trashed {
    pub path: PathBuf,
    pub volume: String,
    /// Unix seconds after which the data is removed.
    pub expires: u64,
}

/// What is in the trash of the pool, the entry that expires first coming first.
pub fn trashed(pool: &Pool) -> io::Result<Vec<Trashed>> {
    let mut entries = Vec::new();
    for path in subdirectories(&pool.path.join(TRASH))? {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        if let Some((volume, expires)) = name.rsplit_once('.')
            && let Ok(expires) = expires.parse()
        {
            entries.push(Trashed { volume: volume.to_string(), expires, path });
        }
    }
    entries.sort_by_key(|entry| entry.expires);
    Ok(entries)
}

/// Removes an entry of the trash for good.
pub fn purge(pool: &Pool, entry: &Trashed) -> io::Result<()> {
    remove_directory(pool, &entry.path)
}

/// Bytes an entry of the trash occupies, when the pool accounts them.
pub fn trashed_bytes(pool: &Pool, entry: &Trashed) -> io::Result<Option<u64>> {
    if !pool.quota {
        return Ok(None);
    }
    let project = quota::project(&entry.path.join(DATA))?.id;
    Ok(Some(quota::usage(&pool.path, project)?.used_bytes))
}

/// Which entries of the trash to remove now: the expired ones, and the oldest of the rest for
/// as long as the filesystem has less free space than the volumes may still claim. The trash
/// does not count as used capacity, so it has to give way.
pub fn purge_candidates(
    trashed: &[(Trashed, Option<u64>)],
    now: u64,
    available_bytes: u64,
    unclaimed_bytes: u64,
) -> Vec<Trashed> {
    let mut available = available_bytes;
    let mut candidates = Vec::new();
    for (entry, bytes) in trashed {
        let expired = entry.expires <= now;
        if expired || available < unclaimed_bytes {
            available = available.saturating_add(bytes.unwrap_or(0));
            candidates.push(entry.clone());
        }
    }
    candidates
}

fn incoming_dir(pool: &Pool, volume: &str) -> PathBuf {
    pool.path.join(INCOMING).join(volume)
}

/// Prepares the directory a copy of the volume is received into, and returns it.
///
/// A copy that waits in the trash of the pool (the volume was on this node before) becomes the
/// start of the new one: only what has changed since has to travel. The copy carries a project
/// of its own, so that it is accounted, and no limit: the limit is set when the volume is handed
/// over. Safe to repeat; a copy that was complete is complete no more.
pub fn prepare_incoming(
    pools: &[Pool],
    project_id_base: u32,
    pool: &Pool,
    volume: &str,
    mode: u32,
) -> io::Result<PathBuf> {
    let dir = incoming_dir(pool, volume);
    let data = dir.join(DATA);
    if !data.is_dir() {
        fs::DirBuilder::new().recursive(true).mode(0o700).create(pool.path.join(INCOMING))?;
        let former = trashed(pool)?.into_iter().rfind(|e| e.volume == volume && e.path.join(DATA).is_dir());
        match former {
            Some(entry) => fs::rename(entry.path, &dir)?,
            None => {
                fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
                fs::create_dir(&data)?;
                fs::set_permissions(&data, fs::Permissions::from_mode(mode))?;
            }
        }
    }
    match fs::remove_file(dir.join(PASSPORT)) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    if pool.quota {
        let project = assign_project(pools, project_id_base, &data)?;
        quota::set_limits(&pool.path, project, Limits::default())?;
    }
    Ok(data)
}

/// Declares the received copy complete: it is flushed to the disk and gets its passport.
pub fn seal_incoming(pool: &Pool, volume: &str, passport: &str) -> io::Result<()> {
    let dir = incoming_dir(pool, volume);
    rustix::fs::syncfs(fs::File::open(dir.join(DATA))?)?;
    write_passport(&dir, passport)
}

/// Whether a complete copy of the volume waits to be handed over.
pub fn arrived(pool: &Pool, volume: &str) -> bool {
    let dir = incoming_dir(pool, volume);
    dir.join(DATA).is_dir() && dir.join(PASSPORT).is_file()
}

/// Makes the received copy the volume. Fails when the pool already has a volume of this name.
pub fn accept(pool: &Pool, volume: &str) -> io::Result<()> {
    fs::DirBuilder::new().recursive(true).mode(0o700).create(pool.path.join(VOLUMES))?;
    // Ids in use are looked for in `volumes`, then `trash`, then `incoming`. Every other move
    // goes along that order and is seen on one side or the other; this one goes against it,
    // and a search under way would miss the directory on both.
    let _moving = lock_projects();
    fs::rename(incoming_dir(pool, volume), volume_dir(pool, volume))
}

/// Removes a received copy, complete or not. Safe to repeat.
pub fn discard_incoming(pool: &Pool, volume: &str) -> io::Result<()> {
    remove_directory(pool, &incoming_dir(pool, volume))
}

/// Names of the volumes that have a copy, complete or not, in `incoming`.
pub fn incoming(pool: &Pool) -> io::Result<Vec<String>> {
    let directories = subdirectories(&pool.path.join(INCOMING))?;
    Ok(directories.iter().filter_map(|d| d.file_name()?.to_str().map(str::to_string)).collect())
}

/// Bytes the received copy occupies so far, when the pool accounts them.
pub fn received_bytes(pool: &Pool, volume: &str) -> io::Result<Option<u64>> {
    if !pool.quota {
        return Ok(None);
    }
    let project = quota::project(&incoming_dir(pool, volume).join(DATA))?.id;
    Ok(Some(quota::usage(&pool.path, project)?.used_bytes))
}

/// Names of the volume directories of the pool.
pub fn list(pool: &Pool) -> io::Result<Vec<String>> {
    let directories = subdirectories(&pool.path.join(VOLUMES))?;
    Ok(directories.iter().filter_map(|d| d.file_name()?.to_str().map(str::to_string)).collect())
}

/// The passport of the volume, as it was written next to its data.
pub fn passport(pool: &Pool, volume: &str) -> io::Result<String> {
    fs::read_to_string(volume_dir(pool, volume).join(PASSPORT))
}

/// What the volume occupies, read from the quota; `None` in a pool without quota, where only the
/// whole filesystem is accounted.
pub fn usage(pool: &Pool, volume: &str) -> io::Result<Option<quota::Usage>> {
    if !pool.quota {
        return Ok(None);
    }
    let project = quota::project(&data_dir(pool, volume))?.id;
    Ok(Some(quota::usage(&pool.path, project)?))
}

/// Bytes the volume occupies, when the pool accounts them.
pub fn used_bytes(pool: &Pool, volume: &str) -> io::Result<Option<u64>> {
    Ok(usage(pool, volume)?.map(|usage| usage.used_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pool without quota works on any filesystem, the one of the temporary directory included.
    fn plain_pool(name: &str) -> Pool {
        let path = std::env::temp_dir().join(format!("local-path-csi-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Pool { name: name.into(), path, quota: false, require_mountpoint: false, overcommit: 1.0 }
    }

    fn params(passport: &str) -> Params<'_> {
        Params { limits: Limits::default(), mode: 0o770, passport }
    }

    #[test]
    fn names_that_would_escape_the_pool_are_invalid() {
        assert!(valid_name("pvc-0f6b0d9e-4f58-4c0a-9d0b-2a2a2f3b8c11"));
        for name in ["", ".", "..", "a/b", "../x", "a\0b"] {
            assert!(!valid_name(name), "{name:?}");
        }
    }

    #[test]
    fn volume_is_a_data_directory_with_the_requested_mode_and_a_passport() {
        let pool = plain_pool("create");
        ensure(std::slice::from_ref(&pool), 1000, &pool, "pv-1", &params("{\"pool\":\"create\"}")).unwrap();
        let data = data_dir(&pool, "pv-1");
        assert_eq!(data, pool.path.join("volumes/pv-1/data"));
        assert_eq!(fs::metadata(&data).unwrap().permissions().mode() & 0o7777, 0o770);
        assert_eq!(fs::read_to_string(pool.path.join("volumes/pv-1/volume.json")).unwrap(), "{\"pool\":\"create\"}");
        assert!(exists(&pool, "pv-1"));
        fs::remove_dir_all(&pool.path).unwrap();
    }

    #[test]
    fn repeating_ensure_keeps_the_data_and_refreshes_the_passport() {
        let pool = plain_pool("repeat");
        let pools = std::slice::from_ref(&pool);
        ensure(pools, 1000, &pool, "pv-1", &params("one")).unwrap();
        fs::write(data_dir(&pool, "pv-1").join("file"), "payload").unwrap();
        ensure(pools, 1000, &pool, "pv-1", &params("two")).unwrap();
        assert_eq!(fs::read_to_string(data_dir(&pool, "pv-1").join("file")).unwrap(), "payload");
        assert_eq!(fs::read_to_string(pool.path.join("volumes/pv-1/volume.json")).unwrap(), "two");
        fs::remove_dir_all(&pool.path).unwrap();
    }

    #[test]
    fn remove_deletes_the_volume_and_may_be_repeated() {
        let pool = plain_pool("remove");
        ensure(std::slice::from_ref(&pool), 1000, &pool, "pv-1", &params("{}")).unwrap();
        fs::write(data_dir(&pool, "pv-1").join("file"), "payload").unwrap();
        remove(&pool, "pv-1").unwrap();
        assert!(!exists(&pool, "pv-1"));
        assert!(!pool.path.join("volumes/pv-1").exists());
        remove(&pool, "pv-1").unwrap();
        fs::remove_dir_all(&pool.path).unwrap();
    }

    #[test]
    fn trashed_volume_keeps_its_data_and_passport_until_it_is_purged() {
        let pool = plain_pool("trash");
        let pools = std::slice::from_ref(&pool);
        ensure(pools, 1000, &pool, "pv-1", &params("passport")).unwrap();
        fs::write(data_dir(&pool, "pv-1").join("file"), "payload").unwrap();
        trash(&pool, "pv-1", 5000).unwrap();
        assert!(!exists(&pool, "pv-1"));
        assert_eq!(list(&pool).unwrap(), Vec::<String>::new());
        // Repeating the move, as a retry of a deletion does, changes nothing.
        trash(&pool, "pv-1", 6000).unwrap();

        let entries = trashed(&pool).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!((entries[0].volume.as_str(), entries[0].expires), ("pv-1", 5000));
        assert_eq!(entries[0].path, pool.path.join("trash/pv-1.5000"));
        assert_eq!(fs::read_to_string(entries[0].path.join("data/file")).unwrap(), "payload");
        assert_eq!(fs::read_to_string(entries[0].path.join("volume.json")).unwrap(), "passport");

        purge(&pool, &entries[0]).unwrap();
        assert!(trashed(&pool).unwrap().is_empty());
        fs::remove_dir_all(&pool.path).unwrap();
    }

    #[test]
    fn volumes_of_one_name_removed_in_the_same_second_do_not_collide() {
        let pool = plain_pool("collide");
        let pools = std::slice::from_ref(&pool);
        for content in ["first", "second"] {
            ensure(pools, 1000, &pool, "pv-1", &params("{}")).unwrap();
            fs::write(data_dir(&pool, "pv-1").join("file"), content).unwrap();
            trash(&pool, "pv-1", 5000).unwrap();
        }
        let entries = trashed(&pool).unwrap();
        assert_eq!(entries.iter().map(|e| e.expires).collect::<Vec<_>>(), [5000, 5001]);
        assert_eq!(fs::read_to_string(entries[0].path.join("data/file")).unwrap(), "first");
        assert_eq!(fs::read_to_string(entries[1].path.join("data/file")).unwrap(), "second");
        fs::remove_dir_all(&pool.path).unwrap();
    }

    #[test]
    fn list_and_passport_describe_what_is_on_the_disk() {
        let pool = plain_pool("list");
        let pools = std::slice::from_ref(&pool);
        assert_eq!(list(&pool).unwrap(), Vec::<String>::new());
        ensure(pools, 1000, &pool, "pv-b", &params("b")).unwrap();
        ensure(pools, 1000, &pool, "pv-a", &params("a")).unwrap();
        assert_eq!(list(&pool).unwrap(), ["pv-a", "pv-b"]);
        assert_eq!(passport(&pool, "pv-a").unwrap(), "a");
        assert!(passport(&pool, "pv-c").is_err());
        fs::remove_dir_all(&pool.path).unwrap();
    }

    #[test]
    fn received_copy_becomes_the_volume_only_when_it_is_sealed() {
        let pool = plain_pool("incoming");
        let pools = std::slice::from_ref(&pool);
        let data = prepare_incoming(pools, 1000, &pool, "pv-1", 0o750).unwrap();
        assert_eq!(data, pool.path.join("incoming/pv-1/data"));
        assert_eq!(fs::metadata(&data).unwrap().permissions().mode() & 0o7777, 0o750);
        fs::write(data.join("file"), "payload").unwrap();
        assert_eq!(incoming(&pool).unwrap(), ["pv-1"]);
        assert!(!arrived(&pool, "pv-1"), "a copy in progress is not a volume yet");
        assert_eq!(list(&pool).unwrap(), Vec::<String>::new());

        seal_incoming(&pool, "pv-1", "passport").unwrap();
        assert!(arrived(&pool, "pv-1"));
        // Starting over on a sealed copy keeps what was received and breaks the seal.
        prepare_incoming(pools, 1000, &pool, "pv-1", 0o750).unwrap();
        assert!(!arrived(&pool, "pv-1"));
        assert_eq!(fs::read_to_string(data.join("file")).unwrap(), "payload");

        seal_incoming(&pool, "pv-1", "passport").unwrap();
        accept(&pool, "pv-1").unwrap();
        assert!(exists(&pool, "pv-1"));
        assert_eq!(fs::read_to_string(data_dir(&pool, "pv-1").join("file")).unwrap(), "payload");
        assert_eq!(passport(&pool, "pv-1").unwrap(), "passport");
        assert_eq!(incoming(&pool).unwrap(), Vec::<String>::new());
        fs::remove_dir_all(&pool.path).unwrap();
    }

    #[test]
    fn received_copy_never_replaces_a_volume_that_is_there() {
        let pool = plain_pool("incoming-clash");
        let pools = std::slice::from_ref(&pool);
        ensure(pools, 1000, &pool, "pv-1", &params("{}")).unwrap();
        fs::write(data_dir(&pool, "pv-1").join("file"), "mine").unwrap();
        let data = prepare_incoming(pools, 1000, &pool, "pv-1", 0o770).unwrap();
        fs::write(data.join("file"), "theirs").unwrap();
        seal_incoming(&pool, "pv-1", "{}").unwrap();
        assert!(accept(&pool, "pv-1").is_err());
        assert_eq!(fs::read_to_string(data_dir(&pool, "pv-1").join("file")).unwrap(), "mine");
        fs::remove_dir_all(&pool.path).unwrap();
    }

    #[test]
    fn copy_left_in_the_trash_is_the_start_of_the_next_one() {
        let pool = plain_pool("incoming-seed");
        let pools = std::slice::from_ref(&pool);
        ensure(pools, 1000, &pool, "pv-1", &params("old passport")).unwrap();
        fs::write(data_dir(&pool, "pv-1").join("file"), "as it was").unwrap();
        trash(&pool, "pv-1", 5000).unwrap();
        ensure(pools, 1000, &pool, "pv-2", &params("{}")).unwrap();
        trash(&pool, "pv-2", 6000).unwrap();

        let data = prepare_incoming(pools, 1000, &pool, "pv-1", 0o770).unwrap();
        assert_eq!(fs::read_to_string(data.join("file")).unwrap(), "as it was");
        assert!(!arrived(&pool, "pv-1"), "the passport of the old copy does not make the new one complete");
        let left: Vec<String> = trashed(&pool).unwrap().into_iter().map(|e| e.volume).collect();
        assert_eq!(left, ["pv-2"], "the trash of other volumes is not touched");
        fs::remove_dir_all(&pool.path).unwrap();
    }

    #[test]
    fn discarded_copy_leaves_nothing_behind() {
        let pool = plain_pool("incoming-discard");
        let data = prepare_incoming(std::slice::from_ref(&pool), 1000, &pool, "pv-1", 0o770).unwrap();
        fs::write(data.join("file"), "half").unwrap();
        discard_incoming(&pool, "pv-1").unwrap();
        assert_eq!(incoming(&pool).unwrap(), Vec::<String>::new());
        discard_incoming(&pool, "pv-1").unwrap();
        fs::remove_dir_all(&pool.path).unwrap();
    }

    fn entry(volume: &str, expires: u64, bytes: u64) -> (Trashed, Option<u64>) {
        (
            Trashed { path: PathBuf::from(format!("/pool/trash/{volume}.{expires}")), volume: volume.into(), expires },
            Some(bytes),
        )
    }

    fn names(candidates: &[Trashed]) -> Vec<&str> {
        candidates.iter().map(|c| c.volume.as_str()).collect()
    }

    #[test]
    fn expired_trash_is_purged_and_the_rest_is_kept_while_there_is_room() {
        let trash = [entry("old", 100, 10), entry("fresh", 900, 10)];
        assert_eq!(names(&purge_candidates(&trash, 500, 1000, 200)), ["old"]);
        assert_eq!(names(&purge_candidates(&trash, 50, 1000, 200)), Vec::<&str>::new());
        assert_eq!(names(&purge_candidates(&trash, 900, 1000, 200)), ["old", "fresh"]);
    }

    #[test]
    fn trash_gives_way_oldest_first_until_the_volumes_have_their_room() {
        // Volumes may still claim 250 bytes, 100 are free: the two oldest entries free 80 + 90.
        let trash = [entry("a", 1000, 80), entry("b", 2000, 90), entry("c", 3000, 500)];
        assert_eq!(names(&purge_candidates(&trash, 0, 100, 250)), ["a", "b"]);
        // In a pool without quota the size of an entry is unknown: everything goes.
        let unknown: Vec<_> = trash.iter().cloned().map(|(e, _)| (e, None)).collect();
        assert_eq!(names(&purge_candidates(&unknown, 0, 100, 250)), ["a", "b", "c"]);
    }

    #[test]
    fn pool_without_quota_reports_no_usage_of_its_own() {
        let pool = plain_pool("usage");
        ensure(std::slice::from_ref(&pool), 1000, &pool, "pv-1", &params("{}")).unwrap();
        assert_eq!(used_bytes(&pool, "pv-1").unwrap(), None);
        fs::remove_dir_all(&pool.path).unwrap();
    }
}
