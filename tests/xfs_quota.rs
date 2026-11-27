//! Quota primitives against a real filesystem.
//!
//! Needs root and a filesystem mounted with project quota at `$LOCAL_PATH_CSI_TEST_POOL`, so the tests
//! are ignored by default; `e2e/xfs-quota.sh` prepares a loop device and runs them.

use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use local_path_csi::pool::{self, Requirements};
use local_path_csi::quota::{self, Limits, Project};

const MIB: u64 = 1024 * 1024;

struct Pool {
    path: PathBuf,
}

fn test_pool() -> Pool {
    let path = PathBuf::from(std::env::var("LOCAL_PATH_CSI_TEST_POOL").expect("LOCAL_PATH_CSI_TEST_POOL is not set"));
    let mountinfo = fs::read_to_string("/proc/self/mountinfo").unwrap();
    pool::check(&mountinfo, &path, Requirements { mountpoint: true, quota: true })
        .expect("the test pool must be a mountpoint with project quota");
    Pool { path }
}

/// A fresh volume directory in its own project.
fn volume(pool: &Pool, project: u32, limits: Limits) -> PathBuf {
    let dir = pool.path.join(format!("volume-{project}"));
    fs::create_dir(&dir).unwrap();
    quota::set_project(&dir, project).unwrap();
    quota::set_limits(&pool.path, project, limits).unwrap();
    dir
}

/// What a directory occupies by itself. Its project is charged for it as for the files in it:
/// nothing on XFS, which keeps the entries of a small directory in the inode, a block on ext4.
fn own_bytes(dir: &Path) -> u64 {
    fs::metadata(dir).unwrap().blocks() * 512
}

/// Writes `megabytes` MiB and reports how many were written before the first error.
fn fill(file: &Path, megabytes: u64) -> (u64, Option<std::io::Error>) {
    let mut out = File::create(file).unwrap();
    let chunk = vec![0xA5_u8; MIB as usize];
    for written in 0..megabytes {
        if let Err(e) = out.write_all(&chunk).and_then(|()| out.sync_data()) {
            return (written, Some(e));
        }
    }
    (megabytes, None)
}

#[test]
#[ignore = "needs root and a pool with project quota"]
fn directory_gets_its_project_and_the_inherit_flag() {
    let pool = test_pool();
    let dir = volume(&pool, 2001, Limits::default());
    assert_eq!(quota::project(&dir).unwrap(), Project { id: 2001, inherit: true });

    fs::create_dir(dir.join("sub")).unwrap();
    File::create(dir.join("sub/file")).unwrap();
    assert_eq!(quota::project(&dir.join("sub")).unwrap(), Project { id: 2001, inherit: true });
    assert_eq!(quota::project(&dir.join("sub/file")).unwrap().id, 2001);
}

#[test]
#[ignore = "needs root and a pool with project quota"]
fn volume_reports_its_limit_as_its_size() {
    let pool = test_pool();
    let dir = volume(&pool, 2002, Limits { bytes: 100 * MIB, inodes: 0 });
    let stats = pool::stats(&dir).unwrap();
    assert_eq!(stats.total_bytes, 100 * MIB);
    assert_eq!(stats.available_bytes, 100 * MIB - own_bytes(&dir));
}

#[test]
#[ignore = "needs root and a pool with project quota"]
fn writes_stop_at_the_limit_even_for_root() {
    let pool = test_pool();
    let dir = volume(&pool, 2003, Limits { bytes: 100 * MIB, inodes: 0 });
    let (written, error) = fill(&dir.join("big"), 200);
    let error = error.expect("200 MiB fitted into a 100 MiB volume");
    // XFS reports a project over its quota as ENOSPC, ext4 as EDQUOT.
    assert!(matches!(error.raw_os_error(), Some(libc::ENOSPC | libc::EDQUOT)), "unexpected error {error}");
    assert!(written <= 100);

    let usage = quota::usage(&pool.path, 2003).unwrap();
    assert_eq!(usage.limit_bytes, 100 * MIB);
    assert!(usage.used_bytes <= 100 * MIB, "used {} bytes", usage.used_bytes);
    assert!(usage.used_bytes >= 99 * MIB, "used {} bytes", usage.used_bytes);
    let stats = pool::stats(&dir).unwrap();
    assert!(stats.available_bytes < MIB, "{} bytes still available", stats.available_bytes);
    assert_eq!(stats.used_bytes, usage.used_bytes);
}

#[test]
#[ignore = "needs root and a pool with project quota"]
fn raising_the_limit_takes_effect_at_once() {
    let pool = test_pool();
    let dir = volume(&pool, 2004, Limits { bytes: 10 * MIB, inodes: 0 });
    assert!(fill(&dir.join("a"), 20).1.is_some());
    quota::set_limits(&pool.path, 2004, Limits { bytes: 100 * MIB, inodes: 0 }).unwrap();
    assert!(fill(&dir.join("b"), 20).1.is_none());
    assert_eq!(pool::stats(&dir).unwrap().total_bytes, 100 * MIB);
}

#[test]
#[ignore = "needs root and a pool with project quota"]
fn inode_limit_caps_the_number_of_files() {
    let pool = test_pool();
    let dir = volume(&pool, 2005, Limits { bytes: 0, inodes: 10 });
    let created = (0..20).take_while(|i| File::create(dir.join(format!("f{i}"))).is_ok()).count();
    // The volume directory itself is the first inode of the project.
    assert_eq!(created, 9);
    assert_eq!(quota::usage(&pool.path, 2005).unwrap().used_inodes, 10);
}

#[test]
#[ignore = "needs root and a pool with project quota"]
fn usage_of_one_volume_does_not_count_against_another() {
    let pool = test_pool();
    let first = volume(&pool, 2006, Limits { bytes: 50 * MIB, inodes: 0 });
    let second = volume(&pool, 2007, Limits { bytes: 50 * MIB, inodes: 0 });
    assert!(fill(&first.join("data"), 30).1.is_none());
    assert!(fill(&second.join("data"), 30).1.is_none());
    assert_eq!(quota::usage(&pool.path, 2006).unwrap().used_bytes, 30 * MIB + own_bytes(&first));
    // A file cannot be renamed into another project: the quota would move silently.
    let error = fs::rename(first.join("data"), second.join("moved")).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::CrossesDevices);
}

mod volume_directories {
    use local_path_csi::config;
    use local_path_csi::volumes::{self, Params};

    use super::*;

    const BASE: u32 = 5000;

    fn quota_pool() -> config::Pool {
        let path = test_pool().path;
        config::Pool { name: "default".into(), path, quota: true, require_mountpoint: true, overcommit: 1.0 }
    }

    fn params(megabytes: u64) -> Params<'static> {
        Params { limits: Limits { bytes: megabytes * MIB, inodes: 0 }, mode: 0o770, passport: "{}" }
    }

    #[test]
    #[ignore = "needs root and a pool with project quota"]
    fn every_volume_gets_a_project_of_its_own_and_its_size() {
        let pool = quota_pool();
        let pools = std::slice::from_ref(&pool);
        volumes::ensure(pools, BASE, &pool, "pv-a", &params(50)).unwrap();
        volumes::ensure(pools, BASE, &pool, "pv-b", &params(80)).unwrap();
        let (a, b) = (volumes::data_dir(&pool, "pv-a"), volumes::data_dir(&pool, "pv-b"));
        let (project_a, project_b) = (quota::project(&a).unwrap(), quota::project(&b).unwrap());
        assert!(project_a.inherit && project_b.inherit);
        assert!(project_a.id >= BASE && project_b.id >= BASE && project_a.id != project_b.id);
        assert_eq!(pool::stats(&a).unwrap().total_bytes, 50 * MIB);
        assert_eq!(pool::stats(&b).unwrap().total_bytes, 80 * MIB);

        // Repeating with a new size is a resize: same project, new limit, data untouched.
        assert!(fill(&a.join("data"), 20).1.is_none());
        volumes::ensure(pools, BASE, &pool, "pv-a", &params(120)).unwrap();
        assert_eq!(quota::project(&a).unwrap(), project_a);
        assert_eq!(pool::stats(&a).unwrap().total_bytes, 120 * MIB);
        assert_eq!(volumes::used_bytes(&pool, "pv-a").unwrap(), Some(20 * MIB + own_bytes(&a)));

        volumes::remove(&pool, "pv-a").unwrap();
        volumes::remove(&pool, "pv-b").unwrap();
    }

    #[test]
    #[ignore = "needs root and a pool with project quota"]
    fn volume_is_published_measured_and_unpublished_as_the_kubelet_asks() {
        use local_path_csi::controller::CONTEXT_POOL;
        use local_path_csi::csi::node_server::Node;
        use local_path_csi::csi::{NodeGetVolumeStatsRequest, NodePublishVolumeRequest, NodeUnpublishVolumeRequest};
        use local_path_csi::node::NodeService;
        use tonic::{Code, Request};

        let pool = quota_pool();
        let pools = std::slice::from_ref(&pool);
        volumes::ensure(pools, BASE, &pool, "pv-s", &params(50)).unwrap();
        volumes::ensure(pools, BASE, &pool, "pv-t", &params(80)).unwrap();
        let config = config::Config { pools: vec![pool.clone()], project_id_base: BASE, transfer: Default::default() };
        let service = NodeService::new("a".into(), std::sync::Arc::new(config));
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let pods = std::env::temp_dir().join(format!("local-path-csi-pods-{}", std::process::id()));
        let target = pods.join("uid-1/mount");

        let publish = |volume: &str, readonly: bool| {
            let request = NodePublishVolumeRequest {
                volume_id: volume.into(),
                target_path: target.display().to_string(),
                volume_context: [(CONTEXT_POOL.to_string(), "default".to_string())].into(),
                readonly,
                ..Default::default()
            };
            runtime.block_on(service.node_publish_volume(Request::new(request))).map(|_| ())
        };
        let unpublish = || {
            let request =
                NodeUnpublishVolumeRequest { volume_id: "pv-s".into(), target_path: target.display().to_string() };
            runtime.block_on(service.node_unpublish_volume(Request::new(request))).map(|_| ())
        };
        let size = |volume: &str, path: &Path| {
            let request = NodeGetVolumeStatsRequest {
                volume_id: volume.into(),
                volume_path: path.display().to_string(),
                ..Default::default()
            };
            let answer = runtime.block_on(service.node_get_volume_stats(Request::new(request)));
            answer.map(|stats| stats.into_inner().usage[0].total as u64).map_err(|refusal| refusal.code())
        };
        let mounts = || {
            let mountinfo = fs::read_to_string("/proc/self/mountinfo").unwrap();
            mountinfo.lines().filter(|line| line.split(' ').nth(4) == target.to_str()).count()
        };

        publish("pv-s", false).unwrap();
        File::create(target.join("file")).unwrap();
        assert!(volumes::data_dir(&pool, "pv-s").join("file").exists(), "the path shows the volume");
        // The kubelet repeats its call: the mount of the first one is found and stays the only one.
        publish("pv-s", false).unwrap();
        assert_eq!(mounts(), 1);
        // A mount that is writable where the call asks for a read-only one is brought in line.
        publish("pv-s", true).unwrap();
        assert_eq!(mounts(), 1);
        assert_eq!(File::create(target.join("other")).unwrap_err().raw_os_error(), Some(30), "EROFS");
        // A path that shows one volume is not taken for another.
        assert_eq!(publish("pv-t", false).unwrap_err().code(), Code::FailedPrecondition);

        // The statistics are the ones of the volume: its size is its limit.
        assert_eq!(size("pv-s", &target), Ok(50 * MIB));
        let link = pods.join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert_eq!(size("pv-s", &link), Ok(50 * MIB), "a path with a symbolic link in it");
        assert_eq!(size("pv-t", &target), Err(Code::NotFound), "the path shows another volume");

        unpublish().unwrap();
        assert!(!target.exists());
        unpublish().unwrap();
        // Where the volume is not, there is a directory of the root filesystem or nothing:
        // neither has statistics of the volume.
        assert_eq!(size("pv-s", &target), Err(Code::NotFound));
        fs::create_dir_all(&target).unwrap();
        assert_eq!(size("pv-s", &target), Err(Code::NotFound));

        fs::remove_dir_all(&pods).unwrap();
        volumes::remove(&pool, "pv-s").unwrap();
        volumes::remove(&pool, "pv-t").unwrap();
    }

    #[test]
    #[ignore = "needs root and a pool with project quota"]
    fn directories_made_at_the_same_time_get_projects_of_their_own() {
        let pool = quota_pool();
        // The agent creates volumes in its main loop while its receiving tasks prepare copies.
        let names: Vec<String> = (0..24).map(|i| format!("pv-race-{i}")).collect();
        let start = std::sync::Barrier::new(names.len());
        let projects: Vec<u32> = std::thread::scope(|scope| {
            let making: Vec<_> = names
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let (pool, start) = (&pool, &start);
                    scope.spawn(move || {
                        let pools = std::slice::from_ref(pool);
                        start.wait();
                        let data = match i % 2 {
                            0 => {
                                volumes::ensure(pools, BASE, pool, name, &params(10)).unwrap();
                                volumes::data_dir(pool, name)
                            }
                            _ => volumes::prepare_incoming(pools, BASE, pool, name, 0o770).unwrap(),
                        };
                        quota::project(&data).unwrap().id
                    })
                })
                .collect();
            making.into_iter().map(|thread| thread.join().unwrap()).collect()
        });
        let distinct: std::collections::BTreeSet<u32> = projects.iter().copied().collect();
        assert_eq!(distinct.len(), names.len(), "project ids: {projects:?}");
        assert!(projects.iter().all(|id| *id >= BASE));
        for (i, name) in names.iter().enumerate() {
            match i % 2 {
                0 => volumes::remove(&pool, name).unwrap(),
                _ => volumes::discard_incoming(&pool, name).unwrap(),
            }
        }
    }

    #[test]
    #[ignore = "needs root and a pool with project quota"]
    fn removed_volume_leaves_neither_data_nor_a_limit_behind() {
        let pool = quota_pool();
        let pools = std::slice::from_ref(&pool);
        volumes::ensure(pools, BASE, &pool, "pv-c", &params(10)).unwrap();
        let project = quota::project(&volumes::data_dir(&pool, "pv-c")).unwrap().id;
        assert!(fill(&volumes::data_dir(&pool, "pv-c").join("data"), 5).1.is_none());
        volumes::remove(&pool, "pv-c").unwrap();
        assert!(!volumes::exists(&pool, "pv-c"));
        let left = quota::usage(&pool.path, project).unwrap();
        assert_eq!((left.limit_bytes, left.used_bytes), (0, 0));

        // The freed project id is taken by the next volume, which must not inherit the old limit.
        volumes::ensure(pools, BASE, &pool, "pv-d", &params(200)).unwrap();
        assert_eq!(quota::project(&volumes::data_dir(&pool, "pv-d")).unwrap().id, project);
        assert!(fill(&volumes::data_dir(&pool, "pv-d").join("data"), 50).1.is_none());
        volumes::remove(&pool, "pv-d").unwrap();
    }

    #[test]
    #[ignore = "needs root and a pool with project quota"]
    fn trashed_volume_stays_charged_until_it_is_purged() {
        let pool = quota_pool();
        let pools = std::slice::from_ref(&pool);
        volumes::ensure(pools, BASE, &pool, "pv-t", &params(50)).unwrap();
        let project = quota::project(&volumes::data_dir(&pool, "pv-t")).unwrap().id;
        assert!(fill(&volumes::data_dir(&pool, "pv-t").join("data"), 20).1.is_none());
        volumes::trash(&pool, "pv-t", 100).unwrap();

        let entries = volumes::trashed(&pool).unwrap();
        let entry = entries.iter().find(|e| e.volume == "pv-t").expect("the volume is in the trash");
        assert_eq!(volumes::trashed_bytes(&pool, entry).unwrap(), Some(20 * MIB + own_bytes(&entry.path.join("data"))));
        assert_eq!(quota::project(&entry.path.join("data")).unwrap().id, project);
        // The project id of what lies in the trash is not handed to a new volume.
        volumes::ensure(pools, BASE, &pool, "pv-u", &params(50)).unwrap();
        assert_ne!(quota::project(&volumes::data_dir(&pool, "pv-u")).unwrap().id, project);

        volumes::purge(&pool, entry).unwrap();
        assert!(volumes::trashed(&pool).unwrap().iter().all(|e| e.volume != "pv-t"));
        assert_eq!(quota::usage(&pool.path, project).unwrap().used_bytes, 0);
        volumes::remove(&pool, "pv-u").unwrap();
    }
}

#[test]
#[ignore = "needs root and a pool with project quota"]
fn read_only_bind_refuses_writes_and_a_mount_is_switched_between_the_two() {
    use local_path_csi::mount;
    let pool = test_pool();
    let (source, target) = (pool.path.join("bind-source"), pool.path.join("bind-target"));
    fs::create_dir(&source).unwrap();
    fs::create_dir(&target).unwrap();
    let read_only = || {
        let mountinfo = fs::read_to_string("/proc/self/mountinfo").unwrap();
        pool::find_mount(&mountinfo, &target).expect("the bind is in mountinfo").read_only
    };

    mount::bind(&source, &target, true).unwrap();
    assert!(read_only());
    let refused = File::create(target.join("file")).unwrap_err();
    assert_eq!(refused.raw_os_error(), Some(30), "EROFS expected, got {refused}");
    // The directory itself stays writable: only this view of it is closed.
    File::create(source.join("written-beside")).unwrap();
    assert!(target.join("written-beside").exists());

    // What NodePublishVolume does about a mount it finds in the wrong state.
    mount::set_read_only(&target, false).unwrap();
    assert!(!read_only());
    File::create(target.join("file")).unwrap();
    mount::set_read_only(&target, true).unwrap();
    assert!(read_only());
    assert!(File::create(target.join("another")).is_err());

    mount::unmount(&target).unwrap();
    assert!(fs::read_dir(&target).unwrap().next().is_none(), "the target is an empty directory again");
    mount::bind(&source, &target, false).unwrap();
    assert!(!read_only());
    File::create(target.join("another")).unwrap();
    mount::unmount(&target).unwrap();
}

#[test]
#[ignore = "needs root and a pool with project quota"]
fn unmounting_what_is_not_mounted_or_not_there_succeeds() {
    use local_path_csi::mount;
    let pool = test_pool();
    // The kubelet repeats NodeUnpublishVolume until it succeeds: a path that was unmounted
    // and removed by an earlier call must not be an error. Here and not among the unit tests:
    // where unmounting is not allowed at all, the answer is "not permitted" for any path.
    mount::unmount(&pool.path.join("never-was-there")).unwrap();
    let plain = pool.path.join("plain-directory");
    fs::create_dir(&plain).unwrap();
    mount::unmount(&plain).unwrap();
    assert!(plain.is_dir(), "a directory that is no mount is left as it is");
}
