use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{validate_bucket_name, BucketNameError};

// ── Global config (--config / SHOEBOX_CONFIG) ─────────────────────────────

/// Global configuration file loaded via `--config` or `SHOEBOX_CONFIG`.
///
/// Supports cross-bucket credentials and shared settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlobalConfig {
    /// Global credentials that apply to all buckets.
    #[serde(default)]
    pub credentials: Vec<Credential>,

    /// Listen address override.
    #[serde(default)]
    pub host: Option<String>,

    /// Listen port override.
    #[serde(default)]
    pub port: Option<u16>,

    /// Bucket paths (alternative to CLI positional args).
    #[serde(default)]
    pub buckets: Vec<PathBuf>,

    /// Capacity of the filesystem watch event channel (default: 1000).
    ///
    /// Increase this for high-churn environments where bulk file operations
    /// (e.g., archive extraction, rsync) generate bursts of events faster
    /// than the watch processor can consume them.
    #[serde(default)]
    pub watch_channel_capacity: Option<usize>,
}

/// Load a global configuration file.
pub async fn load_global_config(path: &Path) -> std::io::Result<GlobalConfig> {
    let contents = tokio::fs::read_to_string(path).await?;
    let config: GlobalConfig = toml::from_str(&contents).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to parse global config {}: {e}", path.display()),
        )
    })?;
    Ok(config)
}

/// Directory name for per-bucket metadata/config.
pub const SHOEBOX_DIR: &str = ".shoebox";
/// Config file within the shoebox directory.
pub const CONFIG_FILE: &str = "config.toml";
/// SQLite database within the shoebox directory.
pub const METADATA_DB: &str = "metadata.db";

// ── Server config ─────────────────────────────────────────────────────────

/// Top-level server configuration.
///
/// When the `binary` feature is enabled this also derives `clap::Parser`
/// so it can be built from CLI args. Library consumers construct it directly.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "binary", derive(clap::Parser))]
#[cfg_attr(
    feature = "binary",
    command(name = "shoebox", about = "Lightweight S3-compatible object storage")
)]
pub struct ServerConfig {
    /// Directories to serve as buckets.
    #[cfg_attr(feature = "binary", arg(required = true))]
    pub paths: Vec<std::path::PathBuf>,

    /// Listen address.
    #[cfg_attr(
        feature = "binary",
        arg(long, default_value = "0.0.0.0", env = "SHOEBOX_HOST")
    )]
    pub host: String,

    /// Listen port.
    #[cfg_attr(
        feature = "binary",
        arg(long, default_value_t = 9000, env = "SHOEBOX_PORT")
    )]
    pub port: u16,

    /// Print secret access keys on startup.
    #[cfg_attr(feature = "binary", arg(long, default_value_t = false))]
    pub show_secrets: bool,

    /// Directory for per-bucket state (config, metadata DB).
    /// When set, state is stored in `<data-dir>/<bucket-name>/` instead of
    /// `<bucket-root>/.shoebox/`.
    #[cfg_attr(feature = "binary", arg(long, env = "SHOEBOX_DATA_DIR"))]
    pub data_dir: Option<std::path::PathBuf>,
}

// ── Per-bucket config (.shoebox/config.toml) ──────────────────────────────

/// Persisted per-bucket configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketConfig {
    /// Bucket name (defaults to directory name).
    #[serde(default)]
    pub bucket_name: Option<String>,

    /// Credentials for this bucket.
    #[serde(default)]
    pub credentials: Vec<Credential>,
}

/// A single access credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credential {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub permissions: Option<Vec<String>>,
}

/// Resolved runtime state for one bucket.
#[derive(Debug, Clone)]
pub struct BucketState {
    /// Validated bucket name.
    pub name: String,
    /// Absolute path to the bucket root directory.
    pub root: PathBuf,
    /// Directory where per-bucket state files live (config.toml, metadata.db).
    pub shoebox_dir: PathBuf,
    /// Persisted config.
    pub config: BucketConfig,
    /// True when the config was generated for the first time this run.
    pub freshly_created: bool,
}

// ── Key generation ────────────────────────────────────────────────────────

/// Generate an S3-style access key ID (20 chars starting with AKIA).
pub fn generate_access_key_id() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut rng = rand::rng();
    let suffix: String = (0..16)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect();
    format!("AKIA{suffix}")
}

/// Generate an S3-style secret access key (40 chars, base64-like).
pub fn generate_secret_access_key() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    (0..40)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

// ── Bucket name derivation ────────────────────────────────────────────────

/// Derive a valid S3 bucket name from a directory path.
///
/// Takes the last path component, lowercases it, replaces invalid chars with
/// hyphens, and validates the result.
pub fn derive_bucket_name(path: &Path) -> Result<String, BucketNameError> {
    let dir_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("bucket");

    let name: String = dir_name
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();

    // Trim leading/trailing hyphens and dots
    let name = name.trim_matches(['-', '.']).to_string();

    if name != dir_name.to_ascii_lowercase() {
        tracing::warn!(
            original = dir_name,
            derived = %name,
            "Bucket name derived from directory name required sanitization"
        );
    }

    // Collapse consecutive hyphens and consecutive dots
    let mut result = String::with_capacity(name.len());
    let mut prev_hyphen = false;
    let mut prev_dot = false;
    for c in name.chars() {
        match c {
            '-' => {
                if !prev_hyphen {
                    result.push(c);
                }
                prev_hyphen = true;
                prev_dot = false;
            }
            '.' => {
                if !prev_dot {
                    result.push(c);
                }
                prev_dot = true;
                prev_hyphen = false;
            }
            _ => {
                result.push(c);
                prev_hyphen = false;
                prev_dot = false;
            }
        }
    }

    // Ensure minimum length
    if result.len() < 3 {
        result = format!("{result}-bucket");
        // Trim again after padding
        result = result.trim_matches('-').to_string();
    }

    validate_bucket_name(&result)?;
    Ok(result)
}

// ── Config loading / generation ───────────────────────────────────────────

/// Load a bucket config from `<shoebox_dir>/config.toml`, or generate a default one.
///
/// If the file doesn't exist, creates it with a generated admin credential.
/// Returns `(config, freshly_created)`.
pub async fn load_or_create_bucket_config(
    shoebox_dir: &Path,
) -> std::io::Result<(BucketConfig, bool)> {
    let config_path = shoebox_dir.join(CONFIG_FILE);

    if config_path.exists() {
        let contents = tokio::fs::read_to_string(&config_path).await?;
        let config: BucketConfig = toml::from_str(&contents).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse {}: {e}", config_path.display()),
            )
        })?;
        return Ok((config, false));
    }

    // Generate default config
    tokio::fs::create_dir_all(&shoebox_dir).await?;

    let config = BucketConfig {
        bucket_name: None,
        credentials: vec![Credential {
            access_key_id: generate_access_key_id(),
            secret_access_key: generate_secret_access_key(),
            description: Some("Full access (admin)".to_string()),
            permissions: None,
        }],
    };

    let toml_str = toml::to_string_pretty(&config)
        .map_err(|e| std::io::Error::other(format!("Failed to serialize config: {e}")))?;

    // Write with restricted permissions from the start to avoid a window
    // where the file containing secrets is world-readable.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&config_path)?;
        std::io::Write::write_all(&mut file, toml_str.as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        tokio::fs::write(&config_path, &toml_str).await?;
    }

    Ok((config, true))
}

/// Save a bucket config to `<shoebox_dir>/config.toml`.
pub async fn save_bucket_config(shoebox_dir: &Path, config: &BucketConfig) -> std::io::Result<()> {
    let config_path = shoebox_dir.join(CONFIG_FILE);
    let toml_str = toml::to_string_pretty(config)
        .map_err(|e| std::io::Error::other(format!("Failed to serialize config: {e}")))?;

    tokio::fs::create_dir_all(shoebox_dir).await?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&config_path)?;
        std::io::Write::write_all(&mut file, toml_str.as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        tokio::fs::write(&config_path, &toml_str).await?;
    }

    Ok(())
}

/// Resolve a directory path into a full `BucketState`.
///
/// When `data_dir` is `Some`, per-bucket state is stored under
/// `<data_dir>/<derived_bucket_name>/` instead of `<bucket_root>/.shoebox/`.
pub async fn resolve_bucket(
    path: &Path,
    data_dir: Option<&Path>,
) -> Result<BucketState, Box<dyn std::error::Error>> {
    let root = tokio::fs::canonicalize(path).await?;

    let derived_name = derive_bucket_name(&root)?;
    let shoebox_dir = match data_dir {
        Some(d) => d.join(&derived_name),
        None => root.join(SHOEBOX_DIR),
    };

    let (config, freshly_created) = load_or_create_bucket_config(&shoebox_dir).await?;

    let name = match &config.bucket_name {
        Some(n) => {
            validate_bucket_name(n)?;
            n.clone()
        }
        None => derived_name,
    };

    Ok(BucketState {
        name,
        root,
        shoebox_dir,
        config,
        freshly_created,
    })
}

// ── Bulk resolution ───────────────────────────────────────────────────────

/// Errors that can occur during startup / bucket resolution.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0} is not a directory")]
    NotADirectory(PathBuf),

    #[error("{0} is read-only; use --data-dir to store state elsewhere")]
    ReadOnlyPath(PathBuf),

    #[error("{0}")]
    Other(#[from] Box<dyn std::error::Error>),
}

impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        Self::Other(err.into())
    }
}

impl From<BucketNameError> for ConfigError {
    fn from(err: BucketNameError) -> Self {
        Self::Other(err.into())
    }
}

/// Validate and resolve every path in the configuration into a [`BucketState`].
///
/// When `data_dir` is `None` and a path is read-only, returns
/// [`ConfigError::ReadOnlyPath`].
pub async fn resolve_all_buckets(config: &ServerConfig) -> Result<Vec<BucketState>, ConfigError> {
    let mut buckets = Vec::with_capacity(config.paths.len());
    for path in &config.paths {
        if !path.is_dir() {
            return Err(ConfigError::NotADirectory(path.clone()));
        }
        if config.data_dir.is_none() {
            // Probe actual writability — permissions().readonly() only checks
            // the owner write bit and is unreliable for non-owner users, ACLs,
            // and read-only mounts.
            if !is_dir_writable(path) {
                return Err(ConfigError::ReadOnlyPath(path.clone()));
            }
        }
        let bucket = resolve_bucket(path, config.data_dir.as_deref())
            .await
            .map_err(|e| match e.downcast_ref::<std::io::Error>() {
                Some(io_err) if io_err.kind() == std::io::ErrorKind::PermissionDenied => {
                    ConfigError::ReadOnlyPath(path.clone())
                }
                _ => ConfigError::Other(e),
            })?;
        buckets.push(bucket);
    }
    Ok(buckets)
}

/// Check if a directory is actually writable by attempting to create and
/// remove a temporary file. This is more reliable than inspecting permission
/// bits, which don't account for effective user identity, ACLs, or read-only
/// mounts.
fn is_dir_writable(path: &Path) -> bool {
    let probe = path.join(".shoebox_write_probe");
    match std::fs::File::create(&probe) {
        Ok(_) => {
            let _ = std::fs::remove_file(&probe);
            true
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_derive_bucket_name_simple() {
        let path = Path::new("/home/user/photos");
        assert_eq!(derive_bucket_name(path).unwrap(), "photos");
    }

    #[test]
    fn test_derive_bucket_name_uppercase() {
        let path = Path::new("/home/user/My-Photos");
        assert_eq!(derive_bucket_name(path).unwrap(), "my-photos");
    }

    #[test]
    fn test_derive_bucket_name_special_chars() {
        let path = Path::new("/home/user/my_cool_photos!");
        assert_eq!(derive_bucket_name(path).unwrap(), "my-cool-photos");
    }

    #[test]
    fn test_derive_bucket_name_short() {
        let path = Path::new("/home/user/ab");
        let name = derive_bucket_name(path).unwrap();
        assert!(name.len() >= 3);
    }

    #[test]
    fn test_generate_access_key_id_format() {
        let key = generate_access_key_id();
        assert!(key.starts_with("AKIA"));
        assert_eq!(key.len(), 20);
    }

    #[test]
    fn test_generate_secret_access_key_length() {
        let key = generate_secret_access_key();
        assert_eq!(key.len(), 40);
    }

    #[tokio::test]
    async fn test_load_or_create_bucket_config_creates_new() {
        let tmp = TempDir::new().unwrap();
        let shoebox_dir = tmp.path().join("my-bucket");
        let (config, freshly_created) = load_or_create_bucket_config(&shoebox_dir).await.unwrap();

        assert!(freshly_created);
        assert_eq!(config.credentials.len(), 1);
        assert!(config.credentials[0].access_key_id.starts_with("AKIA"));

        // File should exist now
        let config_path = shoebox_dir.join(CONFIG_FILE);
        assert!(config_path.exists());

        // Verify 0600 permissions on unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let meta = std::fs::metadata(&config_path).unwrap();
            assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        }
    }

    #[tokio::test]
    async fn test_load_or_create_bucket_config_loads_existing() {
        let tmp = TempDir::new().unwrap();
        let shoebox_dir = tmp.path().join("my-bucket");
        std::fs::create_dir_all(&shoebox_dir).unwrap();

        let config_toml = r#"
bucket_name = "my-custom-bucket"

[[credentials]]
access_key_id = "AKIATEST1234567890AB"
secret_access_key = "testSecretKey1234567890123456789012345678"
description = "Test credential"
"#;
        std::fs::write(shoebox_dir.join(CONFIG_FILE), config_toml).unwrap();

        let (config, freshly_created) = load_or_create_bucket_config(&shoebox_dir).await.unwrap();
        assert!(!freshly_created);
        assert_eq!(config.bucket_name.as_deref(), Some("my-custom-bucket"));
        assert_eq!(config.credentials.len(), 1);
        assert_eq!(config.credentials[0].access_key_id, "AKIATEST1234567890AB");
    }

    #[tokio::test]
    async fn test_resolve_bucket_default_shoebox_dir() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("photos");
        std::fs::create_dir_all(&bucket_root).unwrap();

        let state = resolve_bucket(&bucket_root, None).await.unwrap();
        assert_eq!(state.name, "photos");
        assert_eq!(state.shoebox_dir, state.root.join(SHOEBOX_DIR));
        assert!(state.freshly_created);
        assert!(state.shoebox_dir.join(CONFIG_FILE).exists());
    }

    #[tokio::test]
    async fn test_resolve_bucket_with_data_dir() {
        let tmp = TempDir::new().unwrap();
        let bucket_root = tmp.path().join("photos");
        let data_dir = tmp.path().join("state");
        std::fs::create_dir_all(&bucket_root).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        let state = resolve_bucket(&bucket_root, Some(&data_dir)).await.unwrap();
        assert_eq!(state.name, "photos");
        assert_eq!(state.shoebox_dir, data_dir.join("photos"));
        assert!(state.freshly_created);
        assert!(data_dir.join("photos").join(CONFIG_FILE).exists());
        // .shoebox should NOT exist in the bucket root
        assert!(!bucket_root.join(SHOEBOX_DIR).exists());
    }

    #[tokio::test]
    async fn test_save_bucket_config_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let shoebox_dir = tmp.path().join("roundtrip-bucket");

        let config = BucketConfig {
            bucket_name: Some("my-bucket".to_string()),
            credentials: vec![Credential {
                access_key_id: "AKIATEST1234567890AB".to_string(),
                secret_access_key: "secretkey1234567890123456789012345678".to_string(),
                description: Some("Test cred".to_string()),
                permissions: Some(vec!["read".to_string(), "write".to_string()]),
            }],
        };

        save_bucket_config(&shoebox_dir, &config).await.unwrap();

        // Load it back
        let (loaded, freshly_created) = load_or_create_bucket_config(&shoebox_dir).await.unwrap();
        assert!(!freshly_created);
        assert_eq!(loaded.bucket_name.as_deref(), Some("my-bucket"));
        assert_eq!(loaded.credentials.len(), 1);
        assert_eq!(loaded.credentials[0].access_key_id, "AKIATEST1234567890AB");
        assert_eq!(
            loaded.credentials[0].permissions.as_ref().unwrap(),
            &vec!["read".to_string(), "write".to_string()]
        );

        // Verify 0600 permissions on unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let config_path = shoebox_dir.join(CONFIG_FILE);
            let meta = std::fs::metadata(&config_path).unwrap();
            assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        }
    }

    #[tokio::test]
    async fn test_save_bucket_config_overwrites_existing() {
        let tmp = TempDir::new().unwrap();
        let shoebox_dir = tmp.path().join("overwrite-test");

        let config_v1 = BucketConfig {
            bucket_name: Some("v1".to_string()),
            credentials: vec![],
        };
        save_bucket_config(&shoebox_dir, &config_v1).await.unwrap();

        let config_v2 = BucketConfig {
            bucket_name: Some("v2".to_string()),
            credentials: vec![Credential {
                access_key_id: "AKIANEW".to_string(),
                secret_access_key: "newsecret".to_string(),
                description: None,
                permissions: None,
            }],
        };
        save_bucket_config(&shoebox_dir, &config_v2).await.unwrap();

        let (loaded, _) = load_or_create_bucket_config(&shoebox_dir).await.unwrap();
        assert_eq!(loaded.bucket_name.as_deref(), Some("v2"));
        assert_eq!(loaded.credentials.len(), 1);
    }

    #[tokio::test]
    async fn test_load_global_config() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("shoebox.toml");

        let config_content = r#"
host = "127.0.0.1"
port = 8080
buckets = ["/tmp/photos", "/tmp/docs"]

[[credentials]]
access_key_id = "AKIAGLOBAL1234567890"
secret_access_key = "globalsecret1234567890123456789012345678"
description = "Global admin"
"#;
        std::fs::write(&config_path, config_content).unwrap();

        let config = load_global_config(&config_path).await.unwrap();
        assert_eq!(config.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(config.port, Some(8080));
        assert_eq!(config.credentials.len(), 1);
        assert_eq!(config.credentials[0].access_key_id, "AKIAGLOBAL1234567890");
        assert_eq!(config.buckets.len(), 2);
    }

    #[tokio::test]
    async fn test_load_global_config_minimal() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("minimal.toml");

        // Only buckets, no credentials or server overrides
        std::fs::write(&config_path, "buckets = [\"/tmp/data\"]\n").unwrap();

        let config = load_global_config(&config_path).await.unwrap();
        assert!(config.host.is_none());
        assert!(config.port.is_none());
        assert!(config.credentials.is_empty());
        assert_eq!(config.buckets.len(), 1);
    }

    #[tokio::test]
    async fn test_load_global_config_malformed_returns_error() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("bad.toml");
        std::fs::write(&config_path, "this is not valid toml [[[").unwrap();

        let result = load_global_config(&config_path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_global_config_missing_file_returns_error() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("nonexistent.toml");

        let result = load_global_config(&config_path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_resolve_all_buckets_readonly_without_data_dir() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("readonly-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        // Make the directory read-only
        std::fs::set_permissions(&bucket_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        let config = ServerConfig {
            paths: vec![bucket_dir.clone()],
            host: "127.0.0.1".into(),
            port: 9000,
            show_secrets: false,
            data_dir: None,
        };

        let result = resolve_all_buckets(&config).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("--data-dir"),
            "Error should suggest --data-dir, got: {err_msg}"
        );

        // Restore permissions so TempDir cleanup works
        std::fs::set_permissions(&bucket_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[tokio::test]
    async fn test_resolve_all_buckets_readonly_with_data_dir() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().unwrap();
        let bucket_dir = tmp.path().join("readonly-bucket2");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        let data_dir = tmp.path().join("state");

        // Make the bucket directory read-only
        std::fs::set_permissions(&bucket_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

        let config = ServerConfig {
            paths: vec![bucket_dir.clone()],
            host: "127.0.0.1".into(),
            port: 9000,
            show_secrets: false,
            data_dir: Some(data_dir),
        };

        let result = resolve_all_buckets(&config).await;
        assert!(
            result.is_ok(),
            "Should succeed with --data-dir: {:?}",
            result.err()
        );

        // Restore permissions so TempDir cleanup works
        std::fs::set_permissions(&bucket_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}
