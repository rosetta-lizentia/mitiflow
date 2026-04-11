use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{EmulatorError, Result};
use crate::metrics::{EventManifestEntry, ManifestRole};

#[derive(Debug, Clone)]
pub struct InvariantChecker;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvariantVerdict {
    Pass,
    Fail,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissingEvent {
    pub event_id: String,
    pub publisher_id: String,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateEvent {
    pub event_id: String,
    pub occurrences: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderingViolation {
    pub publisher_id: String,
    pub previous_seq: u64,
    pub current_seq: u64,
    pub event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecksumMismatch {
    pub event_id: String,
    pub published_checksum: u32,
    pub received_checksum: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvariantReport {
    pub total_published: usize,
    pub total_received: usize,
    pub missing_events: Vec<MissingEvent>,
    pub duplicate_events: Vec<DuplicateEvent>,
    pub ordering_violations: Vec<OrderingViolation>,
    pub checksum_mismatches: Vec<ChecksumMismatch>,
    pub verdict: InvariantVerdict,
}

impl InvariantChecker {
    pub fn check(manifest_dir: &Path) -> Result<InvariantReport> {
        let published = load_manifests(manifest_dir, ManifestRole::Published)?;
        let received = load_manifests(manifest_dir, ManifestRole::Received)?;
        let total_published = published.len();

        let mut published_by_id: HashMap<String, EventManifestEntry> =
            HashMap::with_capacity(published.len());
        let mut missing_events = Vec::new();
        for event in published {
            published_by_id
                .entry(event.event_id.clone())
                .or_insert(event);
        }

        let mut received_ids = HashSet::with_capacity(received.len());
        let mut received_counts: HashMap<String, usize> = HashMap::new();
        let mut checksum_mismatches = Vec::new();
        let mut ordering_violations = Vec::new();
        let mut last_seq_by_publisher: HashMap<String, u64> = HashMap::new();

        for event in &received {
            received_ids.insert(event.event_id.clone());

            let count = received_counts.entry(event.event_id.clone()).or_insert(0);
            *count += 1;

            if let Some(previous_seq) =
                last_seq_by_publisher.insert(event.publisher_id.clone(), event.seq)
                && event.seq < previous_seq
            {
                ordering_violations.push(OrderingViolation {
                    publisher_id: event.publisher_id.clone(),
                    previous_seq,
                    current_seq: event.seq,
                    event_id: event.event_id.clone(),
                });
            }

            if let Some(published_event) = published_by_id.get(&event.event_id)
                && published_event.payload_checksum != event.payload_checksum
            {
                checksum_mismatches.push(ChecksumMismatch {
                    event_id: event.event_id.clone(),
                    published_checksum: published_event.payload_checksum,
                    received_checksum: event.payload_checksum,
                });
            }
        }

        for event in published_by_id.values() {
            if !received_ids.contains(&event.event_id) {
                missing_events.push(MissingEvent {
                    event_id: event.event_id.clone(),
                    publisher_id: event.publisher_id.clone(),
                    seq: event.seq,
                });
            }
        }

        let mut duplicate_events = Vec::new();
        for (event_id, occurrences) in received_counts {
            if occurrences > 1 {
                duplicate_events.push(DuplicateEvent {
                    event_id,
                    occurrences,
                });
            }
        }

        missing_events.sort_by(|a, b| a.event_id.cmp(&b.event_id));
        duplicate_events.sort_by(|a, b| a.event_id.cmp(&b.event_id));
        ordering_violations.sort_by(|a, b| {
            a.publisher_id
                .cmp(&b.publisher_id)
                .then(a.current_seq.cmp(&b.current_seq))
        });
        checksum_mismatches.sort_by(|a, b| a.event_id.cmp(&b.event_id));

        let verdict = if missing_events.is_empty()
            && duplicate_events.is_empty()
            && checksum_mismatches.is_empty()
        {
            InvariantVerdict::Pass
        } else {
            InvariantVerdict::Fail
        };

        Ok(InvariantReport {
            total_published,
            total_received: received.len(),
            missing_events,
            duplicate_events,
            ordering_violations,
            checksum_mismatches,
            verdict,
        })
    }
}

fn load_manifests(dir: &Path, role: ManifestRole) -> Result<Vec<EventManifestEntry>> {
    let read_dir = std::fs::read_dir(dir).map_err(|error| {
        EmulatorError::Invariant(format!(
            "failed to read manifest directory '{}': {error}",
            dir.display()
        ))
    })?;

    let mut files = Vec::new();
    for entry in read_dir {
        let entry = entry.map_err(|error| {
            EmulatorError::Invariant(format!(
                "failed to iterate manifest directory '{}': {error}",
                dir.display()
            ))
        })?;

        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "jsonl") {
            files.push(path);
        }
    }

    files.sort();

    let mut out = Vec::new();
    for path in files {
        let file = File::open(&path).map_err(|error| {
            EmulatorError::Invariant(format!(
                "failed to open manifest '{}': {error}",
                path.display()
            ))
        })?;
        let reader = BufReader::new(file);

        for (index, line) in reader.lines().enumerate() {
            let line = line.map_err(|error| {
                EmulatorError::Invariant(format!(
                    "failed to read line {} in '{}': {error}",
                    index + 1,
                    path.display()
                ))
            })?;

            if line.trim().is_empty() {
                continue;
            }

            let entry: EventManifestEntry = serde_json::from_str(&line).map_err(|error| {
                EmulatorError::Invariant(format!(
                    "failed to parse line {} in '{}': {error}",
                    index + 1,
                    path.display()
                ))
            })?;

            if entry.role == role {
                out.push(entry);
            }
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn mk_entry(
        event_id: &str,
        seq: u64,
        publisher_id: &str,
        checksum: u32,
        role: ManifestRole,
    ) -> EventManifestEntry {
        EventManifestEntry {
            event_id: event_id.to_string(),
            seq,
            publisher_id: publisher_id.to_string(),
            partition: 0,
            timestamp: "2026-04-10T12:00:00Z".to_string(),
            payload_checksum: checksum,
            role,
        }
    }

    fn write_manifest(path: &Path, entries: &[EventManifestEntry]) {
        let mut file = File::create(path).unwrap();
        for entry in entries {
            let line = serde_json::to_string(entry).unwrap();
            writeln!(file, "{line}").unwrap();
        }
    }

    #[test]
    fn invariant_zero_loss() {
        let tempdir = tempfile::tempdir().unwrap();

        let published = vec![
            mk_entry("evt-1", 1, "pub-1", 100, ManifestRole::Published),
            mk_entry("evt-2", 2, "pub-1", 200, ManifestRole::Published),
        ];
        let received = vec![
            mk_entry("evt-1", 1, "pub-1", 100, ManifestRole::Received),
            mk_entry("evt-2", 2, "pub-1", 200, ManifestRole::Received),
        ];

        write_manifest(
            &tempdir.path().join("producer-0-published.jsonl"),
            &published,
        );
        write_manifest(&tempdir.path().join("consumer-0-received.jsonl"), &received);

        let report = InvariantChecker::check(tempdir.path()).unwrap();
        assert_eq!(report.total_published, 2);
        assert_eq!(report.total_received, 2);
        assert!(report.missing_events.is_empty());
        assert!(report.duplicate_events.is_empty());
        assert!(report.checksum_mismatches.is_empty());
        assert!(report.ordering_violations.is_empty());
        assert_eq!(report.verdict, InvariantVerdict::Pass);
    }

    #[test]
    fn invariant_missing_events() {
        let tempdir = tempfile::tempdir().unwrap();

        let published: Vec<_> = (0..10)
            .map(|i| {
                mk_entry(
                    &format!("evt-{i}"),
                    i as u64,
                    "pub-1",
                    i as u32,
                    ManifestRole::Published,
                )
            })
            .collect();
        let received: Vec<_> = (0..8)
            .map(|i| {
                mk_entry(
                    &format!("evt-{i}"),
                    i as u64,
                    "pub-1",
                    i as u32,
                    ManifestRole::Received,
                )
            })
            .collect();

        write_manifest(
            &tempdir.path().join("producer-0-published.jsonl"),
            &published,
        );
        write_manifest(&tempdir.path().join("consumer-0-received.jsonl"), &received);

        let report = InvariantChecker::check(tempdir.path()).unwrap();
        assert_eq!(report.verdict, InvariantVerdict::Fail);
        assert_eq!(report.missing_events.len(), 2);
        assert_eq!(report.missing_events[0].event_id, "evt-8");
        assert_eq!(report.missing_events[1].event_id, "evt-9");
    }

    #[test]
    fn invariant_duplicate_events() {
        let tempdir = tempfile::tempdir().unwrap();

        let published = vec![mk_entry(
            "evt-dup",
            1,
            "pub-1",
            123,
            ManifestRole::Published,
        )];
        let received = vec![
            mk_entry("evt-dup", 1, "pub-1", 123, ManifestRole::Received),
            mk_entry("evt-dup", 1, "pub-1", 123, ManifestRole::Received),
            mk_entry("evt-dup", 1, "pub-1", 123, ManifestRole::Received),
        ];

        write_manifest(
            &tempdir.path().join("producer-0-published.jsonl"),
            &published,
        );
        write_manifest(&tempdir.path().join("consumer-0-received.jsonl"), &received);

        let report = InvariantChecker::check(tempdir.path()).unwrap();
        assert_eq!(report.verdict, InvariantVerdict::Fail);
        assert_eq!(report.duplicate_events.len(), 1);
        assert_eq!(report.duplicate_events[0].event_id, "evt-dup");
        assert_eq!(report.duplicate_events[0].occurrences, 3);
    }

    #[test]
    fn invariant_checksum_mismatch() {
        let tempdir = tempfile::tempdir().unwrap();

        let published = vec![mk_entry("evt-1", 1, "pub-1", 111, ManifestRole::Published)];
        let received = vec![mk_entry("evt-1", 1, "pub-1", 999, ManifestRole::Received)];

        write_manifest(
            &tempdir.path().join("producer-0-published.jsonl"),
            &published,
        );
        write_manifest(&tempdir.path().join("consumer-0-received.jsonl"), &received);

        let report = InvariantChecker::check(tempdir.path()).unwrap();
        assert_eq!(report.verdict, InvariantVerdict::Fail);
        assert_eq!(report.checksum_mismatches.len(), 1);
        assert_eq!(report.checksum_mismatches[0].event_id, "evt-1");
        assert_eq!(report.checksum_mismatches[0].published_checksum, 111);
        assert_eq!(report.checksum_mismatches[0].received_checksum, 999);
    }

    #[test]
    fn invariant_ordering_violation() {
        let tempdir = tempfile::tempdir().unwrap();

        let published = vec![
            mk_entry("evt-1", 1, "pub-1", 1, ManifestRole::Published),
            mk_entry("evt-2", 2, "pub-1", 2, ManifestRole::Published),
        ];
        let received = vec![
            mk_entry("evt-2", 2, "pub-1", 2, ManifestRole::Received),
            mk_entry("evt-1", 1, "pub-1", 1, ManifestRole::Received),
        ];

        write_manifest(
            &tempdir.path().join("producer-0-published.jsonl"),
            &published,
        );
        write_manifest(&tempdir.path().join("consumer-0-received.jsonl"), &received);

        let report = InvariantChecker::check(tempdir.path()).unwrap();
        assert_eq!(report.ordering_violations.len(), 1);
        assert_eq!(report.ordering_violations[0].publisher_id, "pub-1");
        assert_eq!(report.ordering_violations[0].previous_seq, 2);
        assert_eq!(report.ordering_violations[0].current_seq, 1);
        assert_eq!(report.verdict, InvariantVerdict::Pass);
    }

    #[test]
    fn invariant_empty_manifests() {
        let tempdir = tempfile::tempdir().unwrap();

        let report = InvariantChecker::check(tempdir.path()).unwrap();
        assert_eq!(report.total_published, 0);
        assert_eq!(report.total_received, 0);
        assert!(report.missing_events.is_empty());
        assert!(report.duplicate_events.is_empty());
        assert!(report.ordering_violations.is_empty());
        assert!(report.checksum_mismatches.is_empty());
        assert_eq!(report.verdict, InvariantVerdict::Pass);
    }
}
