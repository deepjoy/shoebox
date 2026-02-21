use crate::error::S3Error;
use crate::metadata::sqlite::Tag;
use crate::metadata::MetadataStore;

/// Get all tags for an object.
pub async fn get_tags(metadata: &MetadataStore, key: &str) -> Result<Vec<Tag>, S3Error> {
    // Verify the object exists first
    metadata.get_object(key).await?.ok_or(S3Error::NoSuchKey)?;

    metadata.get_object_tags(key).await
}

/// Set tags on an object, replacing any existing tags.
pub async fn put_tags(metadata: &MetadataStore, key: &str, tags: Vec<Tag>) -> Result<(), S3Error> {
    // Verify the object exists first
    metadata.get_object(key).await?.ok_or(S3Error::NoSuchKey)?;

    // Validate limits
    if tags.len() > 10 {
        return Err(S3Error::BadRequest(
            "Maximum 10 tags per object".to_string(),
        ));
    }

    for tag in &tags {
        if tag.key.len() > 128 {
            return Err(S3Error::BadRequest("Tag key too long".to_string()));
        }
        if tag.value.len() > 256 {
            return Err(S3Error::BadRequest("Tag value too long".to_string()));
        }
    }

    // Delete existing tags and insert new ones
    metadata.delete_object_tags(key).await?;

    for tag in tags {
        metadata.insert_object_tag(key, &tag).await?;
    }

    Ok(())
}

/// Delete all tags from an object.
pub async fn delete_tags(metadata: &MetadataStore, key: &str) -> Result<(), S3Error> {
    // Verify the object exists first
    metadata.get_object(key).await?.ok_or(S3Error::NoSuchKey)?;

    metadata.delete_object_tags(key).await
}
