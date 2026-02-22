/// Defines the scope of a scan operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ScanScope {
    /// Specific files by key.
    Files(Vec<String>),

    /// All files under a prefix.
    Subtree { prefix: String },

    /// Entire bucket.
    Bucket,
}

impl ScanScope {
    /// Check whether a given key falls within this scope.
    pub fn includes(&self, key: &str) -> bool {
        match self {
            Self::Files(keys) => keys.iter().any(|k| k == key),
            Self::Subtree { prefix } => key.starts_with(prefix),
            Self::Bucket => true,
        }
    }

    /// Serialise scope type for the `scan_jobs.scope_type` column.
    pub fn scope_type(&self) -> &'static str {
        match self {
            Self::Files(_) => "files",
            Self::Subtree { .. } => "subtree",
            Self::Bucket => "bucket",
        }
    }

    /// Serialise scope data as JSON for the `scan_jobs.scope_data` column.
    pub fn scope_data(&self) -> Option<String> {
        match self {
            Self::Files(keys) => serde_json::to_string(keys).ok(),
            Self::Subtree { prefix } => Some(prefix.clone()),
            Self::Bucket => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_scope_includes_everything() {
        let scope = ScanScope::Bucket;
        assert!(scope.includes("any/key"));
        assert!(scope.includes(""));
    }

    #[test]
    fn test_subtree_scope() {
        let scope = ScanScope::Subtree {
            prefix: "photos/".to_string(),
        };
        assert!(scope.includes("photos/cat.jpg"));
        assert!(scope.includes("photos/sub/dog.png"));
        assert!(!scope.includes("docs/readme.md"));
    }

    #[test]
    fn test_files_scope() {
        let scope = ScanScope::Files(vec!["a.txt".to_string(), "b.txt".to_string()]);
        assert!(scope.includes("a.txt"));
        assert!(scope.includes("b.txt"));
        assert!(!scope.includes("c.txt"));
    }
}
