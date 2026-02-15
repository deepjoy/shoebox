use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{validate_bucket_name, BucketNameError};

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

    /// Whether versioning is enabled.
    #[serde(default)]
    pub versioning_enabled: bool,

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
        versioning_enabled: false,
        credentials: vec![Credential {
            access_key_id: generate_access_key_id(),
            secret_access_key: generate_secret_access_key(),
            description: Some("Full access (admin)".to_string()),
            permissions: None,
        }],
    };

    let toml_str = toml::to_string_pretty(&config).map_err(|e| {
        std::io::Error::other(format!("Failed to serialize config: {e}"))
    })?;

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
            let meta = std::fs::metadata(path)?;
            if meta.permissions().readonly() {
                return Err(ConfigError::ReadOnlyPath(path.clone()));
            }
        }
        let bucket = resolve_bucket(path, config.data_dir.as_deref()).await?;
        buckets.push(bucket);
    }
    Ok(buckets)
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
        assert!(!config.versioning_enabled);
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
versioning_enabled = true

[[credentials]]
access_key_id = "AKIATEST1234567890AB"
secret_access_key = "testSecretKey1234567890123456789012345678"
description = "Test credential"
"#;
        std::fs::write(shoebox_dir.join(CONFIG_FILE), config_toml).unwrap();

        let (config, freshly_created) = load_or_create_bucket_config(&shoebox_dir).await.unwrap();
        assert!(!freshly_created);
        assert_eq!(config.bucket_name.as_deref(), Some("my-custom-bucket"));
        assert!(config.versioning_enabled);
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
}
