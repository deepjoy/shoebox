use serde::{Deserialize, Serialize};

/// XML response for GetObjectTagging.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "Tagging")]
pub struct Tagging {
    #[serde(rename = "TagSet")]
    pub tag_set: TagSet,
}

/// Container for a list of tags.
#[derive(Debug, Serialize, Deserialize)]
pub struct TagSet {
    #[serde(rename = "Tag", default)]
    pub tags: Vec<TagEntry>,
}

/// A single tag key-value pair for XML serialization.
#[derive(Debug, Serialize, Deserialize)]
pub struct TagEntry {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Value")]
    pub value: String,
}

impl From<crate::metadata::sqlite::Tag> for TagEntry {
    fn from(tag: crate::metadata::sqlite::Tag) -> Self {
        Self {
            key: tag.key,
            value: tag.value,
        }
    }
}

impl From<TagEntry> for crate::metadata::sqlite::Tag {
    fn from(entry: TagEntry) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
        }
    }
}
