//! When this process saw the agents report. An agent writes the time of its report by the clock
//! of its node, and the age of a report is how long ago this process saw it change, by a clock
//! that only moves forward: the clocks of two machines are not compared.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

use crate::api::LocalPathNode;

struct Sighting {
    /// The time the agent wrote, as it wrote it.
    reported_at: Option<Time>,
    /// When the report is taken to have been written; `None` while the node has reported nothing.
    since: Option<Instant>,
}

#[derive(Default)]
pub struct Sightings {
    nodes: Mutex<HashMap<String, Sighting>>,
}

impl Sightings {
    /// Takes note of the report `node` carries at `now`.
    ///
    /// A report that is the one seen before changes nothing. One that differs was written just
    /// now. A node seen for the first time - this process has started, or the node is new - is
    /// the one case in which the written time is believed, read at `unix_now`: without it every
    /// node would look alive for a whole timeout after each start of the controller, the ones
    /// whose agents are long gone among them.
    pub fn observe(&self, node: &LocalPathNode, now: Instant, unix_now: u64) {
        let Some(name) = node.metadata.name.as_deref() else { return };
        let status = node.status.as_ref();
        let reported_at = status.and_then(|status| status.reported_at.as_ref());
        let mut nodes = self.nodes.lock().unwrap();
        let since = match nodes.get(name) {
            Some(seen) if seen.reported_at.as_ref() == reported_at => return,
            Some(_) => reported_at.map(|_| now),
            None => {
                let written = status.and_then(|status| status.report_age(unix_now));
                written.map(|age| now.checked_sub(Duration::from_secs(age)).unwrap_or(now))
            }
        };
        nodes.insert(name.to_string(), Sighting { reported_at: reported_at.cloned(), since });
    }

    /// The node is gone from the cluster.
    pub fn forget(&self, name: &str) {
        self.nodes.lock().unwrap().remove(name);
    }

    /// Seconds since the agent of the node was seen to report; `None` when it never was.
    pub fn age(&self, name: &str, now: Instant) -> Option<u64> {
        let since = self.nodes.lock().unwrap().get(name)?.since?;
        Some(now.saturating_duration_since(since).as_secs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{LocalPathNodeStatus, time};

    /// The time of the controller when a test starts, by the calendar.
    const NOW: u64 = 1_795_000_000;
    const TIMEOUT: u64 = 120;

    /// The node as its agent leaves it with a report written at `reported_at`, Unix seconds by
    /// the clock of the node.
    fn node(reported_at: Option<u64>) -> LocalPathNode {
        let mut node = LocalPathNode::new("a", Default::default());
        node.status = Some(LocalPathNodeStatus { reported_at: reported_at.map(time), ..Default::default() });
        node
    }

    fn later(start: Instant, seconds: u64) -> Instant {
        start + Duration::from_secs(seconds)
    }

    #[test]
    fn node_whose_clock_is_behind_is_alive_while_its_agent_reports() {
        let (sightings, start) = (Sightings::default(), Instant::now());
        // Ten minutes behind: by the time it writes, every report is ten minutes old.
        let behind = NOW - 600;
        sightings.observe(&node(Some(behind)), start, NOW);
        assert_eq!(sightings.age("a", start), Some(600), "seen for the first time, the written time is all there is");
        for report in 1..=4 {
            let at = later(start, 30 * report);
            sightings.observe(&node(Some(behind + 30 * report)), at, NOW + 30 * report);
            assert_eq!(sightings.age("a", at), Some(0), "report {report}");
            assert!(sightings.age("a", later(at, 29)).unwrap() <= TIMEOUT);
        }
    }

    #[test]
    fn node_whose_clock_is_ahead_is_dead_once_its_agent_has_stopped() {
        let (sightings, start) = (Sightings::default(), Instant::now());
        // Ten minutes ahead: by the time it writes, the last report is from the future for long.
        let ahead = NOW + 600;
        sightings.observe(&node(Some(ahead)), start, NOW);
        sightings.observe(&node(Some(ahead + 30)), later(start, 30), NOW + 30);
        // And then nothing. The node is seen again and again, with the same report.
        for seconds in [60, 120, 150] {
            sightings.observe(&node(Some(ahead + 30)), later(start, seconds), NOW + seconds);
        }
        assert_eq!(sightings.age("a", later(start, 150)), Some(120));
        assert!(sightings.age("a", later(start, 151)).unwrap() > TIMEOUT);
        assert!(
            node(Some(ahead + 30)).status.unwrap().report_age(NOW + 151).unwrap() <= TIMEOUT,
            "by the written time it would be alive"
        );
    }

    #[test]
    fn node_seen_for_the_first_time_is_as_old_as_its_report_says() {
        let (sightings, start) = (Sightings::default(), Instant::now());
        // The controller has just started, and the agent of this node went away an hour ago.
        sightings.observe(&node(Some(NOW - 3600)), start, NOW);
        assert_eq!(sightings.age("a", start), Some(3600));
        assert_eq!(sightings.age("a", later(start, 10)), Some(3610));
    }

    #[test]
    fn node_that_has_not_reported_has_no_age_until_it_does() {
        let (sightings, start) = (Sightings::default(), Instant::now());
        assert_eq!(sightings.age("a", start), None, "never seen");
        sightings.observe(&node(None), start, NOW);
        assert_eq!(sightings.age("a", later(start, 5)), None, "seen without a report");
        sightings.observe(&node(Some(NOW - 600)), later(start, 10), NOW + 10);
        assert_eq!(sightings.age("a", later(start, 12)), Some(2), "its first report was written just now");
    }

    #[test]
    fn node_that_is_gone_and_back_is_seen_for_the_first_time_again() {
        let (sightings, start) = (Sightings::default(), Instant::now());
        sightings.observe(&node(Some(NOW)), start, NOW);
        sightings.forget("a");
        assert_eq!(sightings.age("a", start), None);
        sightings.observe(&node(Some(NOW - 50)), later(start, 100), NOW + 100);
        assert_eq!(sightings.age("a", later(start, 100)), Some(150));
    }
}
