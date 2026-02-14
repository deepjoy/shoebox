use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{validate_bucket_name, BucketNameError};

/// Directory name for per-bucket metadata/config.
pub const SHOEBOX_DIR: &str = ".shoebox";
/// Config file within the shoebox directory.
pub const CONFIG_FILE: &str = "config.toml";
/// SQLite database within the shoebox directory.
pub const METADATA_DB: &str = "metadata.db";

// ── Server config (CLI only) ──────────────────────────────────────────────

/// Top-level server configuration, built from CLI args and environment.
#[cfg(feature = "binary")]
#[derive(Debug, Clone, clap::Parser)]
#[command(name = "shoebox", about = "Lightweight S3-compatible object storage")]
pub struct ServerConfig {
    /// Directories to serve as buckets.
    #[arg(required = true)]
    pub paths: Vec<std::path::PathBuf>,

    /// Listen address.
    #[arg(long, default_value = "0.0.0.0", env = "SHOEBOX_HOST")]
    pub host: String,

    /// Listen port.
    #[arg(long, default_value_t = 9000, env = "SHOEBOX_PORT")]
    pub port: u16,
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
    pub root: std::path::PathBuf,
    /// Persisted config.
    pub config: BucketConfig,
}

// ── Key generation ────────────────────────────────────────────────────────

/// Generate an S3-style access key ID (20 chars starting with AKIA).
pub fn generate_access_key_id() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut rng = rand::rng();
    let suffix: String = (0..16).map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char).collect();
    format!("AKIA{suffix}")
}

/// Generate an S3-style secret access key (40 chars, base64-like).
pub fn generate_secret_access_key() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut rng = rand::rng();
    (0..40).map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char).collect()
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
