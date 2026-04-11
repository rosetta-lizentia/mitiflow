use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventManifestEntry {
    pub event_id: String,
    pub seq: u64,
    pub publisher_id: String,
    pub partition: u32,
    pub timestamp: String,
    pub payload_checksum: u32,
    pub role: ManifestRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestRole {
    Published,
    Received,
}

pub struct ManifestWriter {
    writer: std::io::BufWriter<std::fs::File>,
    path: PathBuf,
}

impl ManifestWriter {
    pub fn new(dir: &Path, component: &str, instance: usize, role: ManifestRole) -> Result<Self> {
        std::fs::create_dir_all(dir)?;

        let role_str = match role {
            ManifestRole::Published => "published",
            ManifestRole::Received => "received",
        };
        let filename = format!("{component}-{instance}-{role_str}.jsonl");
        let path = dir.join(&filename);
        let file = std::fs::File::create(&path)?;

        Ok(Self {
            writer: std::io::BufWriter::new(file),
            path,
        })
    }

    pub fn write_entry(&mut self, entry: &EventManifestEntry) -> Result<()> {
        let mut line = serde_json::to_string(entry)?;
        line.push('\n');
        self.writer.write_all(line.as_bytes())?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn payload_checksum(payload: &[u8]) -> u32 {
    crc32fast::hash(payload)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChaosManifestEntry {
    pub timestamp: String,
    pub elapsed_ms: u64,
    pub seed: Option<u64>,
    pub sequence: u64,
    pub source: ChaosEventSource,
    pub action: String,
    pub target: String,
    pub instance: Option<usize>,
    pub parameters: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChaosEventSource {
    Fixed,
    Random,
}

pub struct ChaosManifestWriter {
    writer: std::io::BufWriter<std::fs::File>,
    path: PathBuf,
    sequence: u64,
}

impl ChaosManifestWriter {
    pub fn new(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;

        let filename = "chaos-manifest.jsonl";
        let path = dir.join(filename);
        let file = std::fs::File::create(&path)?;

        Ok(Self {
            writer: std::io::BufWriter::new(file),
            path,
            sequence: 0,
        })
    }

    pub fn write_entry(&mut self, entry: &ChaosManifestEntry) -> Result<()> {
        let stamped = ChaosManifestEntry {
            sequence: self.sequence,
            ..entry.clone()
        };
        let mut line = serde_json::to_string(&stamped)?;
        line.push('\n');
        self.writer.write_all(line.as_bytes())?;
        self.sequence += 1;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_manifest_entry_roundtrip() {
        let entry = EventManifestEntry {
            event_id: "evt-1".to_string(),
            seq: 42,
            publisher_id: "pub-7".to_string(),
            partition: 3,
            timestamp: "2026-04-10T12:00:00Z".to_string(),
            payload_checksum: payload_checksum(b"hello world"),
            role: ManifestRole::Published,
        };

        let json = serde_json::to_string(&entry).unwrap();
        let decoded: EventManifestEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(entry, decoded);
    }

    #[test]
    fn manifest_writer_creates_file_at_expected_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir = tempdir.path();

        let writer = ManifestWriter::new(dir, "producer", 2, ManifestRole::Received).unwrap();

        assert_eq!(writer.path(), &dir.join("producer-2-received.jsonl"));
        assert!(writer.path().exists());
    }

    #[test]
    fn manifest_writer_writes_valid_jsonl() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir = tempdir.path();

        let mut writer = ManifestWriter::new(dir, "consumer", 0, ManifestRole::Published).unwrap();

        let entry1 = EventManifestEntry {
            event_id: "evt-1".to_string(),
            seq: 1,
            publisher_id: "pub-1".to_string(),
            partition: 0,
            timestamp: "2026-04-10T12:00:00Z".to_string(),
            payload_checksum: payload_checksum(b"one"),
            role: ManifestRole::Published,
        };
        let entry2 = EventManifestEntry {
            event_id: "evt-2".to_string(),
            seq: 2,
            publisher_id: "pub-1".to_string(),
            partition: 0,
            timestamp: "2026-04-10T12:00:01Z".to_string(),
            payload_checksum: payload_checksum(b"two"),
            role: ManifestRole::Published,
        };

        writer.write_entry(&entry1).unwrap();
        writer.write_entry(&entry2).unwrap();
        writer.flush().unwrap();

        let content = std::fs::read_to_string(writer.path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 2);
        for line in lines {
            let decoded: EventManifestEntry = serde_json::from_str(line).unwrap();
            assert_eq!(decoded.role, ManifestRole::Published);
        }
    }

    #[test]
    fn chaos_manifest_entry_roundtrip() {
        let entry = ChaosManifestEntry {
            timestamp: "2026-04-10T12:00:00Z".to_string(),
            elapsed_ms: 5_000,
            seed: Some(42),
            sequence: 7,
            source: ChaosEventSource::Random,
            action: "kill".to_string(),
            target: "store-1".to_string(),
            instance: Some(0),
            parameters: serde_json::json!({"restart_after_ms": 5000}),
        };

        let json = serde_json::to_string(&entry).unwrap();
        let decoded: ChaosManifestEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(entry, decoded);
    }

    #[test]
    fn chaos_manifest_writer_creates_file() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir = tempdir.path();

        let writer = ChaosManifestWriter::new(dir).unwrap();

        assert_eq!(writer.path(), &dir.join("chaos-manifest.jsonl"));
        assert!(writer.path().exists());
    }

    #[test]
    fn chaos_manifest_writer_writes_valid_jsonl() {
        let tempdir = tempfile::tempdir().unwrap();
        let dir = tempdir.path();
        let mut writer = ChaosManifestWriter::new(dir).unwrap();
        let path = writer.path().to_path_buf();

        let entry1 = ChaosManifestEntry {
            timestamp: "2026-04-10T12:00:00Z".to_string(),
            elapsed_ms: 5_000,
            seed: Some(42),
            sequence: 999,
            source: ChaosEventSource::Random,
            action: "kill".to_string(),
            target: "store-1".to_string(),
            instance: Some(0),
            parameters: serde_json::json!({"restart_after_ms": 5000}),
        };
        let entry2 = ChaosManifestEntry {
            timestamp: "2026-04-10T12:00:01Z".to_string(),
            elapsed_ms: 6_000,
            seed: Some(42),
            sequence: 999,
            source: ChaosEventSource::Fixed,
            action: "pause".to_string(),
            target: "producer".to_string(),
            instance: Some(1),
            parameters: serde_json::json!({"duration_ms": 1000}),
        };
        let entry3 = ChaosManifestEntry {
            timestamp: "2026-04-10T12:00:02Z".to_string(),
            elapsed_ms: 7_000,
            seed: Some(42),
            sequence: 999,
            source: ChaosEventSource::Random,
            action: "resume".to_string(),
            target: "producer".to_string(),
            instance: None,
            parameters: serde_json::json!({"reason": "recovered"}),
        };

        writer.write_entry(&entry1).unwrap();
        writer.write_entry(&entry2).unwrap();
        writer.write_entry(&entry3).unwrap();
        writer.flush().unwrap();

        let content = std::fs::read_to_string(path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 3);

        let decoded1: ChaosManifestEntry = serde_json::from_str(lines[0]).unwrap();
        let decoded2: ChaosManifestEntry = serde_json::from_str(lines[1]).unwrap();
        let decoded3: ChaosManifestEntry = serde_json::from_str(lines[2]).unwrap();

        assert_eq!(decoded1.sequence, 0);
        assert_eq!(decoded1.action, "kill");
        assert_eq!(decoded1.source, ChaosEventSource::Random);
        assert_eq!(
            decoded1.parameters,
            serde_json::json!({"restart_after_ms": 5000})
        );

        assert_eq!(decoded2.sequence, 1);
        assert_eq!(decoded2.action, "pause");
        assert_eq!(decoded2.source, ChaosEventSource::Fixed);
        assert_eq!(
            decoded2.parameters,
            serde_json::json!({"duration_ms": 1000})
        );

        assert_eq!(decoded3.sequence, 2);
        assert_eq!(decoded3.action, "resume");
        assert_eq!(decoded3.instance, None);
        assert_eq!(
            decoded3.parameters,
            serde_json::json!({"reason": "recovered"})
        );
    }
}
