use clap::Parser;
use shoebox::config::{resolve_bucket, ServerConfig};

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

    let config = ServerConfig::parse();

    // Resolve each path into a bucket
    let mut buckets = Vec::new();
    for path in &config.paths {
        if !path.is_dir() {
            eprintln!("Error: {} is not a directory", path.display());
            std::process::exit(1);
        }
        let bucket = resolve_bucket(path).await?;
        buckets.push(bucket);
    }

    // Print startup info
    println!(
        "Serving {} bucket{} on http://{}:{}",
        buckets.len(),
        if buckets.len() == 1 { "" } else { "s" },
        config.host,
        config.port
    );
    println!();

    for bucket in &buckets {
        let show = config.show_secrets || bucket.freshly_created;
        println!("  {} → {}", bucket.name, bucket.root.display());
        if bucket.freshly_created {
            println!("    (new) Credentials generated:");
        } else {
            println!("    Credentials:");
        }
        for (i, cred) in bucket.config.credentials.iter().enumerate() {
            let desc = cred
                .description
                .as_deref()
                .unwrap_or("no description");
            println!(
                "      [{}] {} ({})",
                i + 1,
                cred.access_key_id,
                desc
            );
            if show {
                println!("          Secret: {}", cred.secret_access_key);
            }
        }
        println!();
    }

    println!("Credentials saved to .shoebox/config.toml");
    if !config.show_secrets {
        println!("Use --show-secrets to display secret access keys");
    }

    // TODO: Add the Axum router and server here
    tracing::info!("Server startup complete (no HTTP listener yet — Phase 2)");

    Ok(())
}
