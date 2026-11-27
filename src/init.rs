//! The process the agent runs under.
//!
//! In its container the agent is process 1, and whatever loses its parent there becomes a child
//! of process 1. A copy that is given up leaves such processes: its rsync is killed, and the
//! receiver and the remote shell that rsync had started outlive it. Nobody waited for them when
//! they ended, and they stayed, dead, for as long as the agent ran.
//!
//! The agent cannot wait for them itself: a wait for any child takes the exit status of the
//! processes the runtime waits for, and a copy that went well would be reported as failed. So the
//! agent is two processes. The first one only waits: for the agent, and for everything that was
//! left to it. The second one is the agent: the same program, started over by the first.

use std::ffi::OsStr;
use std::io;
use std::os::unix::process::CommandExt;
use std::process::Command;

use rustix::process::{Pid, Signal, WaitOptions, WaitStatus};
use tokio::signal::unix::{SignalKind, signal};

/// Tells a process that it is the agent, and names the process it runs under.
const SUPERVISOR: &str = "LOCAL_PATH_CSI_SUPERVISOR";

/// Makes two processes of the one that was started, and returns in the one that goes on to be
/// the agent. The other one, the process that was started, never returns: it starts the program
/// over as the agent, collects every process that is left without a parent, passes on the
/// signals that stop the agent, and ends when the agent ends - with its exit code, or with 128
/// and the number of the signal that killed it.
///
/// Call it before the process has a runtime: the process that waits brings its own.
pub fn supervise() -> io::Result<()> {
    if let Some(supervisor) = std::env::var_os(SUPERVISOR) {
        return supervised(&supervisor);
    }
    // What loses its parent below this process comes to it, whether or not it is process 1.
    rustix::process::set_child_subreaper(Some(rustix::process::getpid()))?;
    // On the thread that calls: the agent is told when the thread that started it ends, and
    // this one ends with the process.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_io().build()?;
    let code = runtime.block_on(watch())?;
    std::process::exit(code)
}

/// The agent: makes sure that it does not outlive the process it runs under.
fn supervised(supervisor: &OsStr) -> io::Result<()> {
    // An agent nobody waits for is not left running.
    rustix::process::set_parent_process_death_signal(Some(Signal::TERM))?;
    // The process may have gone before that was asked for.
    let parent = rustix::process::getppid().map(|parent| parent.as_raw_nonzero().to_string());
    if parent.as_deref() != supervisor.to_str() {
        return Err(io::Error::other("the process the agent runs under is gone"));
    }
    Ok(())
}

/// How the process ends when the agent has ended with `status`.
fn code(status: WaitStatus) -> i32 {
    match (status.exit_status(), status.terminating_signal()) {
        (Some(code), _) => code,
        (None, Some(signal)) => 128 + signal,
        (None, None) => 1,
    }
}

/// Starts the agent and waits: returns the code to end with once the agent has ended.
async fn watch() -> io::Result<i32> {
    // Before the agent is started: no signal meant for it, and no word of its end, is missed.
    let mut ended = signal(SignalKind::child())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut arguments = std::env::args_os();
    let name = arguments.next().unwrap_or_else(|| "local-path-csi".into());
    // The program of this process, whatever has become of the file it was started from.
    let agent = Command::new("/proc/self/exe")
        .arg0(name)
        .args(arguments)
        .env(SUPERVISOR, std::process::id().to_string())
        .spawn()?;
    let agent = Pid::from_child(&agent);
    loop {
        // One signal may stand for several processes that have ended. A child that is not the
        // agent was left to this process, and the wait is all it needed.
        while let Some((process, status)) = rustix::process::wait(WaitOptions::NOHANG)? {
            if process == agent {
                return Ok(code(status));
            }
        }
        let stopping = tokio::select! {
            _ = ended.recv() => continue,
            _ = terminate.recv() => Signal::TERM,
            _ = interrupt.recv() => Signal::INT,
        };
        // The agent has not been waited for: its number is still its own.
        rustix::process::kill_process(agent, stopping)?;
    }
}
