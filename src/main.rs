use std::path::PathBuf;

use clap::{Parser, Subcommand};
use shoebox::auth::presigned;
use shoebox::config::{
    generate_access_key_id, generate_secret_access_key, load_or_create_bucket_config,
    save_bucket_config, Credential,
};
use shoebox::error::ShoeboxError;

/// Top-level CLI -- subcommands OR serve mode, never both.
#[derive(Parser)]
#[command(
    name = "shoebox",
    about = "Lightweight S3-compatible object storage",
    version,
    args_conflicts_with_subcommands = true,
    subcommand_negates_reqs = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    serve: ServeArgs,
}

/// Arguments for the default serve mode (`shoebox <PATH>...`).
#[derive(Debug, Clone, clap::Args)]
struct ServeArgs {
    /// Directories to serve as buckets.
    #[arg(required = false)]
    paths: Vec<PathBuf>,

    /// Listen address.
    #[arg(long, default_value = "0.0.0.0", env = "SHOEBOX_HOST")]
    host: String,

    /// Listen port.
    #[arg(long, default_value_t = 9000, env = "SHOEBOX_PORT")]
    port: u16,

    /// Print secret access keys on startup.
    #[arg(long, default_value_t = false)]
    show_secrets: bool,

    /// Directory for per-bucket state (config, metadata DB).
    #[arg(long, env = "SHOEBOX_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Path to global config file.
    #[arg(long, env = "SHOEBOX_CONFIG")]
    config: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Commands {
    /// Add a new credential to a bucket.
    AddCredential {
        bucket_path: PathBuf,
        #[arg(long, default_value = "admin")]
        permissions: String,
        #[arg(long)]
        description: Option<String>,
        /// Port to check for running server (for warning).
        #[arg(long, default_value_t = 9000)]
        port: u16,
    },

    /// List credentials for a bucket.
    ListCredentials {
        bucket_path: PathBuf,
        /// Port to check for running server (for warning).
        #[arg(long, default_value_t = 9000)]
        port: u16,
    },

    /// Remove a credential from a bucket.
    RemoveCredential {
        bucket_path: PathBuf,
        access_key_id: String,
        /// Port to check for running server (for warning).
        #[arg(long, default_value_t = 9000)]
        port: u16,
    },

    /// Rename (move) an object within a bucket.
    #[command(alias = "mv")]
    Rename {
        /// Path to the bucket directory.
        bucket_path: PathBuf,
        /// Source object key.
        source_key: String,
        /// Destination object key.
        dest_key: String,
        /// Overwrite if destination exists.
        #[arg(long)]
        overwrite: bool,
    },

    /// Generate a pre-signed URL.
    Presign {
        #[command(subcommand)]
        action: PresignAction,
    },
}

#[derive(Subcommand)]
enum PresignAction {
    /// Generate a pre-signed GET (download) URL.
    Get {
        bucket: String,
        key: String,
        #[arg(long, default_value = "1h")]
        expires: String,
        #[arg(long, default_value = "http://localhost:9000")]
        endpoint: String,
        #[arg(long)]
        bucket_path: PathBuf,
    },
    /// Generate a pre-signed PUT (upload) URL.
    Put {
        bucket: String,
        key: String,
        #[arg(long, default_value = "1h")]
        expires: String,
        #[arg(long, default_value = "http://localhost:9000")]
        endpoint: String,
        #[arg(long)]
        bucket_path: PathBuf,
        #[arg(long)]
        content_type: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SHOEBOX_LOG")
                .or_else(|_| tracing_subscriber::EnvFilter::try_from_env("RUST_LOG"))
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // Handle subcommands
    if let Some(command) = cli.command {
        return handle_command(command).await;
    }

    // Serve mode
    let serve = cli.serve;

    // Build Shoebox via the library API
    let mut builder = shoebox::Shoebox::builder()
        .host(&serve.host)
        .port(serve.port);

    if let Some(ref data_dir) = serve.data_dir {
        builder = builder.data_dir(data_dir);
    }

    if let Some(ref config_path) = serve.config {
        builder = builder.config_file(config_path);
    }

    for path in &serve.paths {
        builder = builder.bucket(path);
    }

    let shoebox = builder.build().await.map_err(cli_error_message)?;

    // Print startup banner
    let buckets = shoebox.loaded_buckets();
    let mut names: Vec<&String> = buckets.keys().collect();
    names.sort();
    for name in &names {
        let bucket = &buckets[*name];
        println!("  {} -> {}", bucket.name, bucket.storage.root().display());
        let show = serve.show_secrets || bucket.freshly_created;
        if bucket.freshly_created {
            println!("    (new) Credentials generated:");
        } else {
            println!("    Credentials:");
        }
        for (i, cred) in bucket.config.credentials.iter().enumerate() {
            let desc = cred.description.as_deref().unwrap_or("no description");
            println!("      [{}] {} ({})", i + 1, cred.access_key_id, desc);
            if show {
                println!("          Secret: {}", cred.secret_access_key);
            }
        }
        println!();
    }

    let listen_addr = format!("{}:{}", shoebox.host(), shoebox.port());
    println!(
        "Serving {} bucket{} on http://{}",
        buckets.len(),
        if buckets.len() == 1 { "" } else { "s" },
        listen_addr,
    );
    if let Some(ref data_dir) = serve.data_dir {
        println!("Credentials saved to {}/*/config.toml", data_dir.display());
    } else {
        println!("Credentials saved to .shoebox/config.toml");
    }
    if !serve.show_secrets {
        println!("Use --show-secrets to display secret access keys");
    }
    println!();

    shoebox.run().await.map_err(cli_error_message)?;
    Ok(())
}

/// Translate library-level [`ShoeboxError`] variants into CLI-friendly messages.
fn cli_error_message(e: ShoeboxError) -> Box<dyn std::error::Error> {
    match e {
        ShoeboxError::PortInUse { port } => format!(
            "Port {port} is already in use. Is another Shoebox instance running?\n\
             Try a different port with --port <PORT>"
        )
        .into(),
        ShoeboxError::PermissionDenied { path } => format!(
            "{}: permission denied; use --data-dir to store state elsewhere",
            path.display()
        )
        .into(),
        other => other.into(),
    }
}

async fn handle_command(command: Commands) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Commands::AddCredential {
            bucket_path,
            permissions,
            description,
            port,
        } => {
            warn_if_server_running(port);
            let shoebox_dir = resolve_shoebox_dir(&bucket_path);
            let (mut config, _) = load_or_create_bucket_config(&shoebox_dir).await?;

            let access_key_id = generate_access_key_id();
            let secret_access_key = generate_secret_access_key();
            let perm_list: Vec<String> = permissions
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();

            config.credentials.push(Credential {
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                description: description.clone(),
                permissions: Some(perm_list.clone()),
            });

            save_bucket_config(&shoebox_dir, &config).await?;

            println!("Credential added:");
            println!("  Access Key ID: {}", access_key_id);
            println!("  Secret Access Key: {}", secret_access_key);
            println!("  Permissions: {}", permissions);
            if let Some(desc) = description {
                println!("  Description: {}", desc);
            }
        }

        Commands::ListCredentials { bucket_path, port } => {
            warn_if_server_running(port);
            let shoebox_dir = resolve_shoebox_dir(&bucket_path);
            let (config, _) = load_or_create_bucket_config(&shoebox_dir).await?;

            if config.credentials.is_empty() {
                println!("No credentials configured for {}", bucket_path.display());
                return Ok(());
            }

            println!("Credentials for {}:", bucket_path.display());
            for (i, cred) in config.credentials.iter().enumerate() {
                let desc = cred.description.as_deref().unwrap_or("no description");
                let perms = cred
                    .permissions
                    .as_ref()
                    .map(|p| p.join(","))
                    .unwrap_or_else(|| "admin".to_string());
                println!(
                    "  [{}] {} ({}) [{}]",
                    i + 1,
                    cred.access_key_id,
                    desc,
                    perms
                );
            }
        }

        Commands::RemoveCredential {
            bucket_path,
            access_key_id,
            port,
        } => {
            warn_if_server_running(port);
            let shoebox_dir = resolve_shoebox_dir(&bucket_path);
            let (mut config, _) = load_or_create_bucket_config(&shoebox_dir).await?;

            let before = config.credentials.len();
            config
                .credentials
                .retain(|c| c.access_key_id != access_key_id);
            let after = config.credentials.len();

            if before == after {
                eprintln!("Credential {} not found", access_key_id);
                std::process::exit(1);
            }

            save_bucket_config(&shoebox_dir, &config).await?;
            println!("Credential {} removed", access_key_id);
        }

        Commands::Rename {
            bucket_path,
            source_key,
            dest_key,
            overwrite,
        } => {
            let shoebox = shoebox::Shoebox::builder()
                .bucket(&bucket_path)
                .build()
                .await?;

            let bucket_name = bucket_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or("Invalid bucket path")?;

            shoebox
                .rename_object(bucket_name, &source_key, &dest_key, overwrite)
                .await
                .map_err(|e| format!("Rename failed: {}", e))?;

            println!("Renamed {} -> {}", source_key, dest_key);
        }

        Commands::Presign { action } => match action {
            PresignAction::Get {
                bucket,
                key,
                expires,
                endpoint,
                bucket_path,
            } => {
                let expires_secs = presigned::parse_duration(&expires)
                    .map_err(|e| format!("Invalid duration: {}", e))?;
                let cred = get_first_credential(&bucket_path).await?;
                let url = presigned::generate_presigned_get(
                    &endpoint,
                    &bucket,
                    &key,
                    &cred.access_key_id,
                    &cred.secret_access_key,
                    expires_secs,
                );
                println!("{}", url);
            }
            PresignAction::Put {
                bucket,
                key,
                expires,
                endpoint,
                bucket_path,
                content_type,
            } => {
                let expires_secs = presigned::parse_duration(&expires)
                    .map_err(|e| format!("Invalid duration: {}", e))?;
                let cred = get_first_credential(&bucket_path).await?;
                let url = presigned::generate_presigned_put(
                    &endpoint,
                    &bucket,
                    &key,
                    &cred.access_key_id,
                    &cred.secret_access_key,
                    expires_secs,
                    content_type.as_deref(),
                );
                println!("{}", url);
            }
        },
    }

    Ok(())
}

/// Resolve the .shoebox directory for a given bucket path.
fn resolve_shoebox_dir(bucket_path: &std::path::Path) -> PathBuf {
    bucket_path.join(".shoebox")
}

/// Get the first credential from a bucket's config.
async fn get_first_credential(
    bucket_path: &std::path::Path,
) -> Result<Credential, Box<dyn std::error::Error>> {
    let shoebox_dir = resolve_shoebox_dir(bucket_path);
    let (config, _) = load_or_create_bucket_config(&shoebox_dir).await?;
    config
        .credentials
        .into_iter()
        .next()
        .ok_or_else(|| "No credentials found for this bucket".into())
}

/// Check if a server is running on the given port and print a warning.
fn warn_if_server_running(port: u16) {
    if std::net::TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        std::time::Duration::from_millis(200),
    )
    .is_ok()
    {
        eprintln!(
            "Warning: A Shoebox server appears to be running on port {}.",
            port
        );
        eprintln!("  Changes will take effect on next restart, or call:");
        eprintln!("  curl -X POST http://localhost:{}/_shoebox/reload", port);
        eprintln!();
    }
}
