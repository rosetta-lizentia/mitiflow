//! Custom serde helpers for config deserialization edge cases.

use serde::{Deserialize, Deserializer};

/// Deserialize `managed_topics` from a YAML list of topic name strings.
///
/// This exists so the `ComponentDef.managed_topics` field can use `alias = "topics"`
/// without conflicting with the top-level `TopologyConfig.topics` (which is `Vec<TopicDef>`).
/// Both serialize as YAML sequences, but the component-level field expects plain strings.
pub fn deserialize_managed_topics<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer)
}
