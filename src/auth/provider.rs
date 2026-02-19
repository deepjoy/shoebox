use std::collections::HashMap;

use crate::config::{BucketConfig, Credential};

/// S3 operation categories for permission checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    Admin,
    Read,
    Write,
    Sync,
}

impl Permission {
    pub fn parse(s: &str) -> Option<Permission> {
        match s.to_lowercase().as_str() {
            "admin" => Some(Permission::Admin),
            "read" => Some(Permission::Read),
            "write" => Some(Permission::Write),
            "sync" => Some(Permission::Sync),
            _ => None,
        }
    }
}

/// A credential resolved at startup, ready for auth checks.
#[derive(Debug, Clone)]
pub struct ResolvedCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub permissions: Vec<Permission>,
    /// Which bucket this credential belongs to (None = global/all buckets).
    pub bucket_name: Option<String>,
    pub description: Option<String>,
}

impl ResolvedCredential {
    /// Check if this credential allows the given operation on the given bucket.
    pub fn has_permission(&self, operation: &str, bucket: &str) -> bool {
        // 1. If scoped to a specific bucket, check it matches
        if let Some(ref b) = self.bucket_name {
            if b != bucket {
                return false;
            }
        }

        // 2. Empty permissions (legacy Phase 1 creds) = admin
        if self.permissions.is_empty() {
            return true;
        }

        // 3. Check each permission shortcut
        for perm in &self.permissions {
            match perm {
                Permission::Admin => return true,
                Permission::Read => {
                    if is_read_operation(operation) {
                        return true;
                    }
                }
                Permission::Write => {
                    if is_write_operation(operation) {
                        return true;
                    }
                }
                Permission::Sync => {
                    if operation == "SyncBucket" {
                        return true;
                    }
                }
            }
        }
        false
    }
}

fn is_read_operation(op: &str) -> bool {
    matches!(
        op,
        "ListBuckets"
            | "HeadBucket"
            | "GetBucketLocation"
            | "GetBucketVersioning"
            | "ListObjectsV2"
            | "GetObject"
            | "HeadObject"
            | "ListParts"
            | "ListMultipartUploads"
    )
}

fn is_write_operation(op: &str) -> bool {
    matches!(
        op,
        "PutObject"
            | "DeleteObject"
            | "DeleteObjects"
            | "InitiateMultipartUpload"
            | "UploadPart"
            | "CompleteMultipartUpload"
            | "AbortMultipartUpload"
    )
}

/// Resolve a `Credential` from config into a `ResolvedCredential`.
fn resolve_credential(cred: &Credential, bucket_name: Option<String>) -> ResolvedCredential {
    let permissions = cred
        .permissions
        .as_ref()
        .map(|perms| perms.iter().filter_map(|s| Permission::parse(s)).collect())
        .unwrap_or_default();

    ResolvedCredential {
        access_key_id: cred.access_key_id.clone(),
        secret_access_key: cred.secret_access_key.clone(),
        permissions,
        bucket_name,
        description: cred.description.clone(),
    }
}

/// Index of all credentials, keyed by access_key_id for O(1) lookup.
pub struct CredentialProvider {
    credentials: HashMap<String, ResolvedCredential>,
}

impl CredentialProvider {
    /// Build from loaded bucket configurations at startup.
    pub fn from_buckets(buckets: &[(String, &BucketConfig)]) -> Self {
        let mut credentials = HashMap::new();
        for (bucket_name, config) in buckets {
            for cred in &config.credentials {
                let resolved = resolve_credential(cred, Some(bucket_name.clone()));
                credentials.insert(resolved.access_key_id.clone(), resolved);
            }
        }
        Self { credentials }
    }

    /// Create an empty provider.
    pub fn empty() -> Self {
        Self {
            credentials: HashMap::new(),
        }
    }

    pub fn lookup(&self, access_key_id: &str) -> Option<&ResolvedCredential> {
        self.credentials.get(access_key_id)
    }

    /// Add or replace a credential at runtime.
    pub fn insert(&mut self, credential: ResolvedCredential) {
        self.credentials
            .insert(credential.access_key_id.clone(), credential);
    }

    /// Remove a credential by access key ID. Returns true if it existed.
    pub fn remove(&mut self, access_key_id: &str) -> bool {
        self.credentials.remove(access_key_id).is_some()
    }

    /// List all credentials.
    pub fn list(&self) -> Vec<&ResolvedCredential> {
        self.credentials.values().collect()
    }

    /// Replace all credentials (used by reload).
    pub fn replace_all(&mut self, new: CredentialProvider) {
        self.credentials = new.credentials;
    }

    /// Check if there would still be at least one admin credential
    /// remaining after removing the given access key.
    pub fn has_other_admin(&self, access_key_id: &str) -> bool {
        self.credentials.iter().any(|(k, v)| {
            k != access_key_id
                && (v.permissions.is_empty() || v.permissions.contains(&Permission::Admin))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cred(perms: &[&str], bucket: Option<&str>) -> ResolvedCredential {
        ResolvedCredential {
            access_key_id: "AKIATEST".to_string(),
            secret_access_key: "secret".to_string(),
            permissions: perms.iter().filter_map(|s| Permission::parse(s)).collect(),
            bucket_name: bucket.map(|s| s.to_string()),
            description: None,
        }
    }

    #[test]
    fn admin_allows_everything() {
        let cred = make_cred(&["admin"], None);
        assert!(cred.has_permission("GetObject", "photos"));
        assert!(cred.has_permission("PutObject", "photos"));
        assert!(cred.has_permission("DeleteObject", "docs"));
        assert!(cred.has_permission("SyncBucket", "photos"));
    }

    #[test]
    fn read_only_blocks_writes() {
        let cred = make_cred(&["read"], None);
        assert!(cred.has_permission("GetObject", "photos"));
        assert!(cred.has_permission("ListObjectsV2", "photos"));
        assert!(cred.has_permission("HeadObject", "photos"));
        assert!(!cred.has_permission("PutObject", "photos"));
        assert!(!cred.has_permission("DeleteObject", "photos"));
    }

    #[test]
    fn write_only_blocks_reads() {
        let cred = make_cred(&["write"], None);
        assert!(cred.has_permission("PutObject", "photos"));
        assert!(cred.has_permission("DeleteObject", "photos"));
        assert!(!cred.has_permission("GetObject", "photos"));
        assert!(!cred.has_permission("ListObjectsV2", "photos"));
    }

    #[test]
    fn bucket_scoped_credential() {
        let cred = make_cred(&["admin"], Some("photos"));
        assert!(cred.has_permission("GetObject", "photos"));
        assert!(!cred.has_permission("GetObject", "docs"));
    }

    #[test]
    fn empty_permissions_is_admin() {
        let cred = make_cred(&[], None);
        assert!(cred.has_permission("GetObject", "photos"));
        assert!(cred.has_permission("PutObject", "photos"));
        assert!(cred.has_permission("DeleteObject", "photos"));
    }

    #[test]
    fn provider_lookup() {
        let config = BucketConfig {
            bucket_name: None,
            versioning_enabled: false,
            credentials: vec![Credential {
                access_key_id: "AKIATEST1234567890AB".to_string(),
                secret_access_key: "secret123".to_string(),
                description: Some("test".to_string()),
                permissions: Some(vec!["read".to_string()]),
            }],
        };
        let provider = CredentialProvider::from_buckets(&[("photos".to_string(), &config)]);
        assert!(provider.lookup("AKIATEST1234567890AB").is_some());
        assert!(provider.lookup("NONEXISTENT").is_none());
    }

    #[test]
    fn provider_insert_remove() {
        let mut provider = CredentialProvider::empty();
        let cred = ResolvedCredential {
            access_key_id: "AKIATEST".to_string(),
            secret_access_key: "secret".to_string(),
            permissions: vec![],
            bucket_name: None,
            description: None,
        };
        provider.insert(cred);
        assert!(provider.lookup("AKIATEST").is_some());
        assert!(provider.remove("AKIATEST"));
        assert!(provider.lookup("AKIATEST").is_none());
        assert!(!provider.remove("AKIATEST"));
    }

    #[test]
    fn test_has_other_admin_single_admin() {
        let config = BucketConfig {
            bucket_name: None,
            versioning_enabled: false,
            credentials: vec![Credential {
                access_key_id: "AKIAONLY1234567890AB".to_string(),
                secret_access_key: "secret".to_string(),
                description: None,
                permissions: Some(vec!["admin".to_string()]),
            }],
        };
        let provider = CredentialProvider::from_buckets(&[("photos".to_string(), &config)]);
        // Cannot delete the sole admin
        assert!(!provider.has_other_admin("AKIAONLY1234567890AB"));
    }

    #[test]
    fn test_has_other_admin_multiple_admins() {
        let config = BucketConfig {
            bucket_name: None,
            versioning_enabled: false,
            credentials: vec![
                Credential {
                    access_key_id: "AKIAADMIN1XXXXXXXXXX".to_string(),
                    secret_access_key: "secret1".to_string(),
                    description: None,
                    permissions: Some(vec!["admin".to_string()]),
                },
                Credential {
                    access_key_id: "AKIAADMIN2XXXXXXXXXX".to_string(),
                    secret_access_key: "secret2".to_string(),
                    description: None,
                    permissions: Some(vec!["admin".to_string()]),
                },
            ],
        };
        let provider = CredentialProvider::from_buckets(&[("photos".to_string(), &config)]);
        // Can delete either one because the other still exists
        assert!(provider.has_other_admin("AKIAADMIN1XXXXXXXXXX"));
        assert!(provider.has_other_admin("AKIAADMIN2XXXXXXXXXX"));
    }

    #[test]
    fn test_has_other_admin_legacy_empty_perms() {
        // Empty permissions (legacy Phase 1 creds) = admin
        let config = BucketConfig {
            bucket_name: None,
            versioning_enabled: false,
            credentials: vec![Credential {
                access_key_id: "AKIALEGACY0000000000".to_string(),
                secret_access_key: "secret".to_string(),
                description: None,
                permissions: None,
            }],
        };
        let provider = CredentialProvider::from_buckets(&[("photos".to_string(), &config)]);
        // Legacy cred with no permissions is treated as admin — cannot delete last
        assert!(!provider.has_other_admin("AKIALEGACY0000000000"));
    }

    #[test]
    fn test_has_other_admin_read_only_not_admin() {
        let config = BucketConfig {
            bucket_name: None,
            versioning_enabled: false,
            credentials: vec![
                Credential {
                    access_key_id: "AKIAADMIN000000000AB".to_string(),
                    secret_access_key: "secret1".to_string(),
                    description: None,
                    permissions: Some(vec!["admin".to_string()]),
                },
                Credential {
                    access_key_id: "AKIAREAD0000000000AB".to_string(),
                    secret_access_key: "secret2".to_string(),
                    description: None,
                    permissions: Some(vec!["read".to_string()]),
                },
            ],
        };
        let provider = CredentialProvider::from_buckets(&[("photos".to_string(), &config)]);
        // Cannot delete the admin — the read-only cred is not an admin
        assert!(!provider.has_other_admin("AKIAADMIN000000000AB"));
        // Can delete the read-only cred
        assert!(provider.has_other_admin("AKIAREAD0000000000AB"));
    }

    #[test]
    fn provider_replace_all() {
        let mut provider = CredentialProvider::empty();
        provider.insert(ResolvedCredential {
            access_key_id: "AKIAOLD".to_string(),
            secret_access_key: "old".to_string(),
            permissions: vec![],
            bucket_name: None,
            description: None,
        });

        let mut new_provider = CredentialProvider::empty();
        new_provider.insert(ResolvedCredential {
            access_key_id: "AKIANEW".to_string(),
            secret_access_key: "new".to_string(),
            permissions: vec![Permission::Read],
            bucket_name: None,
            description: None,
        });

        provider.replace_all(new_provider);

        // Old credential gone, new one present
        assert!(provider.lookup("AKIAOLD").is_none());
        assert!(provider.lookup("AKIANEW").is_some());
    }

    #[test]
    fn provider_list_returns_all() {
        let config = BucketConfig {
            bucket_name: None,
            versioning_enabled: false,
            credentials: vec![
                Credential {
                    access_key_id: "AKIA1111111111111111".to_string(),
                    secret_access_key: "s1".to_string(),
                    description: None,
                    permissions: None,
                },
                Credential {
                    access_key_id: "AKIA2222222222222222".to_string(),
                    secret_access_key: "s2".to_string(),
                    description: None,
                    permissions: None,
                },
            ],
        };
        let provider = CredentialProvider::from_buckets(&[("photos".to_string(), &config)]);
        assert_eq!(provider.list().len(), 2);
    }

    #[test]
    fn sync_permission() {
        let cred = make_cred(&["sync"], None);
        assert!(cred.has_permission("SyncBucket", "photos"));
        assert!(!cred.has_permission("GetObject", "photos"));
        assert!(!cred.has_permission("PutObject", "photos"));
    }
}
