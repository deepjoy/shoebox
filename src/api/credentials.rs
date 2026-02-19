use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;

use crate::auth::provider::{CredentialProvider, Permission, ResolvedCredential};
use crate::config::{
    generate_access_key_id, generate_secret_access_key, load_or_create_bucket_config,
    save_bucket_config, Credential,
};
use crate::error::S3Error;
use crate::services::AppState;

/// POST /_shoebox/credentials — Create a new credential.
pub async fn create_credential(
    State(state): State<AppState>,
    body: String,
) -> Result<Response, S3Error> {
    // Check admin permission from the request extension
    check_admin_from_request_not_needed();

    // Parse the XML request body
    let bucket_name = extract_xml_field(&body, "BucketName");
    let permissions_str = extract_xml_field(&body, "Permissions");
    let description = extract_xml_field(&body, "Description");

    let access_key_id = generate_access_key_id();
    let secret_access_key = generate_secret_access_key();

    let permissions: Vec<Permission> = permissions_str
        .as_deref()
        .unwrap_or("admin")
        .split(',')
        .filter_map(|s| Permission::parse(s.trim()))
        .collect();

    let permission_strings: Vec<String> = permissions_str
        .as_deref()
        .unwrap_or("admin")
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let resolved = ResolvedCredential {
        access_key_id: access_key_id.clone(),
        secret_access_key: secret_access_key.clone(),
        permissions,
        bucket_name: bucket_name.clone(),
        description: description.clone(),
    };

    // Insert into in-memory provider
    {
        let mut provider = state.credential_provider.write().await;
        provider.insert(resolved);
    }

    // Persist to disk
    if let Some(ref bname) = bucket_name {
        if let Ok(bucket) = state.get_bucket(bname) {
            let mut config = bucket.config.clone();
            config.credentials.push(Credential {
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                description: description.clone(),
                permissions: Some(permission_strings.clone()),
            });
            let shoebox_dir = find_shoebox_dir_for_bucket(&state, bname);
            if let Some(dir) = shoebox_dir {
                let _ = save_bucket_config(&dir, &config).await;
            }
        }
    }

    let perms_display = permission_strings.join(",");
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateCredentialResult>
  <AccessKeyId>{}</AccessKeyId>
  <SecretAccessKey>{}</SecretAccessKey>{}{}
</CreateCredentialResult>"#,
        access_key_id,
        secret_access_key,
        bucket_name
            .as_ref()
            .map(|b| format!("\n  <BucketName>{}</BucketName>", b))
            .unwrap_or_default(),
        if !perms_display.is_empty() {
            format!(
                "\n  <Permissions>{}</Permissions>{}",
                perms_display,
                description
                    .as_ref()
                    .map(|d| format!("\n  <Description>{}</Description>", d))
                    .unwrap_or_default()
            )
        } else {
            String::new()
        },
    );

    Ok(axum::http::Response::builder()
        .status(StatusCode::CREATED)
        .header("content-type", "application/xml")
        .body(axum::body::Body::from(xml))
        .unwrap())
}

/// Query parameters for list credentials.
#[derive(Debug, serde::Deserialize, Default)]
pub struct ListCredentialsParams {
    pub bucket: Option<String>,
}

/// GET /_shoebox/credentials — List all credentials (secrets redacted).
pub async fn list_credentials(
    State(state): State<AppState>,
    Query(params): Query<ListCredentialsParams>,
) -> Result<Response, S3Error> {
    let provider = state.credential_provider.read().await;
    let creds = provider.list();

    let mut xml =
        String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListCredentialsResult>\n");

    for cred in creds {
        // Filter by bucket if requested
        if let Some(ref filter_bucket) = params.bucket {
            match &cred.bucket_name {
                Some(b) if b != filter_bucket => continue,
                None => continue,
                _ => {}
            }
        }

        let perms: Vec<&str> = cred
            .permissions
            .iter()
            .map(|p| match p {
                Permission::Admin => "admin",
                Permission::Read => "read",
                Permission::Write => "write",
                Permission::Sync => "sync",
            })
            .collect();

        xml.push_str("  <Credential>\n");
        xml.push_str(&format!(
            "    <AccessKeyId>{}</AccessKeyId>\n",
            cred.access_key_id
        ));
        xml.push_str("    <SecretAccessKey>REDACTED</SecretAccessKey>\n");
        if let Some(ref b) = cred.bucket_name {
            xml.push_str(&format!("    <BucketName>{}</BucketName>\n", b));
        }
        if !perms.is_empty() {
            xml.push_str(&format!(
                "    <Permissions>{}</Permissions>\n",
                perms.join(",")
            ));
        }
        if let Some(ref d) = cred.description {
            xml.push_str(&format!("    <Description>{}</Description>\n", d));
        }
        xml.push_str("  </Credential>\n");
    }

    xml.push_str("</ListCredentialsResult>");

    Ok(axum::http::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(axum::body::Body::from(xml))
        .unwrap())
}

/// DELETE /_shoebox/credentials/{access_key_id} — Delete a credential.
pub async fn delete_credential(
    State(state): State<AppState>,
    Path(access_key_id): Path<String>,
) -> Result<Response, S3Error> {
    let mut provider = state.credential_provider.write().await;

    // Safety check: prevent deleting the last admin credential
    if !provider.has_other_admin(&access_key_id) {
        return Err(S3Error::AccessDenied);
    }

    let removed = provider.remove(&access_key_id);
    drop(provider);

    if !removed {
        return Err(S3Error::NoSuchCredential);
    }

    // Persist removal to disk - find which bucket config contains this credential
    for (_, bucket) in state.buckets.iter() {
        if bucket
            .config
            .credentials
            .iter()
            .any(|c| c.access_key_id == access_key_id)
        {
            let mut config = bucket.config.clone();
            config
                .credentials
                .retain(|c| c.access_key_id != access_key_id);
            let shoebox_dir = find_shoebox_dir_for_bucket(&state, &bucket.name);
            if let Some(dir) = shoebox_dir {
                let _ = save_bucket_config(&dir, &config).await;
            }
        }
    }

    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteCredentialResult>
  <AccessKeyId>{}</AccessKeyId>
  <Deleted>true</Deleted>
</DeleteCredentialResult>"#,
        access_key_id
    );

    Ok(axum::http::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(axum::body::Body::from(xml))
        .unwrap())
}

/// POST /_shoebox/reload — Reload config from disk.
pub async fn reload_config(State(state): State<AppState>) -> Result<Response, S3Error> {
    let mut new_provider = CredentialProvider::empty();
    let mut cred_count = 0u32;
    let bucket_count = state.buckets.len() as u32;

    for (_, bucket) in state.buckets.iter() {
        let shoebox_dir = find_shoebox_dir_for_bucket(&state, &bucket.name);
        if let Some(dir) = shoebox_dir {
            match load_or_create_bucket_config(&dir).await {
                Ok((config, _)) => {
                    let bucket_creds: Vec<(String, &crate::config::BucketConfig)> =
                        vec![(bucket.name.clone(), &config)];
                    // We need to build from the reloaded config
                    let temp_provider = CredentialProvider::from_buckets(
                        &bucket_creds
                            .iter()
                            .map(|(n, c)| (n.clone(), *c))
                            .collect::<Vec<_>>(),
                    );
                    for cred in temp_provider.list() {
                        cred_count += 1;
                        new_provider.insert(cred.clone());
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to reload config for bucket {}: {}", bucket.name, e);
                    return Err(S3Error::InternalError);
                }
            }
        }
    }

    // Atomic swap
    let mut provider = state.credential_provider.write().await;
    provider.replace_all(new_provider);

    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ReloadResult>
  <CredentialsLoaded>{}</CredentialsLoaded>
  <BucketsReloaded>{}</BucketsReloaded>
</ReloadResult>"#,
        cred_count, bucket_count
    );

    Ok(axum::http::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(axum::body::Body::from(xml))
        .unwrap())
}

/// Find the shoebox directory for a bucket by scanning loaded buckets.
fn find_shoebox_dir_for_bucket(state: &AppState, bucket_name: &str) -> Option<std::path::PathBuf> {
    state.buckets.get(bucket_name).map(|b| {
        // Check if the storage root has a .shoebox directory
        let root = b.storage.root();
        let shoebox_in_root = root.join(".shoebox");
        if shoebox_in_root.exists() {
            shoebox_in_root
        } else {
            // Fallback: look in the root itself (data_dir case)
            // The shoebox_dir is stored relative to data_dir/{bucket_name}
            // We can reconstruct it from the metadata DB path
            root.join(".shoebox")
        }
    })
}

/// Extract a field from simple XML.
fn extract_xml_field(xml: &str, field: &str) -> Option<String> {
    let open = format!("<{}>", field);
    let close = format!("</{}>", field);
    let start = xml.find(&open)?;
    let end = xml.find(&close)?;
    let value = &xml[start + open.len()..end];
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

/// Placeholder: admin check is done by the auth middleware.
/// The middleware inserts a ResolvedCredential extension,
/// and `determine_operation` maps /_shoebox/ paths to "Admin".
fn check_admin_from_request_not_needed() {
    // Auth middleware handles this via determine_operation() → "Admin"
    // which requires Permission::Admin.
}
