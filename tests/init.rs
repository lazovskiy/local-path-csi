//! The process the agent runs under: it collects the processes the agent leaves behind, passes
//! on the signals that stop the agent, and ends as the agent ended.
//!
//! No test harness: the harness runs tests on threads, and what is tested here forks. The
//! binary starts itself over, and the copy plays the agent.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Tells the copy what the agent it plays does.
const ROLE: &str = "LOCAL_PATH_CSI_TEST_AGENT";

static STOPPED: AtomicBool = AtomicBool::new(false);

extern "C" fn stop(_signal: libc::c_int) {
    STOPPED.store(true, Ordering::Relaxed);
}

/// What the copy of the binary does: an agent, started the way `local-path-csi agent` starts.
fn agent(role: &str) -> ! {
    local_path_csi::init::supervise().expect("supervise");
    match role {
        "leaves-a-process-behind" => {
            // SAFETY: the process has one thread; the calls are plain system calls.
            unsafe {
                let child = libc::fork();
                assert!(child >= 0, "fork");
                if child == 0 {
                    let parent = libc::getpid();
                    let orphan = libc::fork();
                    assert!(orphan >= 0, "fork");
                    if orphan > 0 {
                        // The parent of the orphan goes at once, as a killed rsync does.
                        libc::_exit(0);
                    }
                    while libc::getppid() == parent {
                        libc::usleep(1000);
                    }
                    println!("{} {}", libc::getpid(), libc::getppid());
                    // Lives until the test says so.
                    let mut word = [0u8; 1];
                    let _ = std::io::stdin().read(&mut word);
                    libc::_exit(0);
                }
                let mut status = 0;
                libc::waitpid(child, &mut status, 0);
                // The agent lives on: it is not the end of the agent that collects the orphan.
                loop {
                    libc::pause();
                }
            }
        }
        "ends-with-a-code" => std::process::exit(7),
        "is-killed" => {
            // SAFETY: a signal to the process itself.
            unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
            unreachable!("killed");
        }
        "stops-when-told" => {
            // SAFETY: the handler only stores into an atomic.
            unsafe { libc::signal(libc::SIGTERM, stop as extern "C" fn(libc::c_int) as libc::sighandler_t) };
            println!("ready");
            while !STOPPED.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(5));
            }
            println!("stopped");
            std::process::exit(0);
        }
        other => panic!("no such role: {other}"),
    }
}

/// The copy that plays the agent. A test that fails leaves none of it behind.
struct Started(Child);

impl Drop for Started {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts the copy that plays the agent. What is started is the process `local-path-csi agent` is:
/// the one the kubelet signals and whose end is the end of the container.
fn start(role: &str) -> Started {
    let copy = Command::new(std::env::current_exe().unwrap())
        .env(ROLE, role)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    Started(copy)
}

/// A line of what the agent and its processes print. A test that hears nothing fails.
fn line(output: &mut impl BufRead) -> String {
    let mut line = String::new();
    output.read_line(&mut line).unwrap();
    assert!(!line.is_empty(), "the agent ended without a word");
    line.trim().to_string()
}

fn eventually(seconds: u64, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    done()
}

fn process_an_agent_leaves_behind_is_collected() {
    let mut started = start("leaves-a-process-behind");
    let mut output = BufReader::new(started.0.stdout.take().unwrap());
    let said = line(&mut output);
    let (orphan, parent) = said.split_once(' ').unwrap();
    assert_eq!(parent, started.0.id().to_string(), "the orphan belongs to the process the agent runs under");
    let orphan = Path::new("/proc").join(orphan);
    assert!(orphan.exists(), "the orphan lives until it is told to go");

    started.0.stdin.take().unwrap().write_all(b"x").unwrap();
    // Dead and not collected, it would stay in /proc for as long as its parent lives.
    assert!(eventually(20, || !orphan.exists()), "the orphan is collected when it ends");
    assert!(started.0.try_wait().unwrap().is_none(), "and that is not for the agent having ended");
}

fn agent_that_ends_with_a_code_ends_the_process_with_it() {
    let status = start("ends-with-a-code").0.wait().unwrap();
    assert_eq!(status.code(), Some(7));
}

fn agent_killed_by_a_signal_ends_the_process_as_a_shell_would_report_it() {
    let status = start("is-killed").0.wait().unwrap();
    assert_eq!(status.code(), Some(128 + libc::SIGKILL));
}

fn signal_to_stop_reaches_the_agent() {
    let mut started = start("stops-when-told");
    let mut output = BufReader::new(started.0.stdout.take().unwrap());
    assert_eq!(line(&mut output), "ready");
    // SAFETY: a signal to a process this test has started and not yet waited for.
    unsafe { libc::kill(started.0.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(line(&mut output), "stopped", "the agent stops in its own way");
    assert_eq!(started.0.wait().unwrap().code(), Some(0));
}

fn main() {
    if let Ok(role) = std::env::var(ROLE) {
        agent(&role);
    }
    let tests: [(&str, fn()); 4] = [
        ("process_an_agent_leaves_behind_is_collected", process_an_agent_leaves_behind_is_collected),
        ("agent_that_ends_with_a_code_ends_the_process_with_it", agent_that_ends_with_a_code_ends_the_process_with_it),
        (
            "agent_killed_by_a_signal_ends_the_process_as_a_shell_would_report_it",
            agent_killed_by_a_signal_ends_the_process_as_a_shell_would_report_it,
        ),
        ("signal_to_stop_reaches_the_agent", signal_to_stop_reaches_the_agent),
    ];
    let mut failed = 0;
    for (name, test) in tests {
        match std::panic::catch_unwind(test) {
            Ok(()) => println!("test {name} ... ok"),
            Err(_) => {
                println!("test {name} ... FAILED");
                failed += 1;
            }
        }
    }
    println!("\ntest result: {} passed; {failed} failed", tests.len() - failed);
    std::process::exit(i32::from(failed > 0));
}
