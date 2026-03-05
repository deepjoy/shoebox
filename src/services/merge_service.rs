use crate::error::S3Error;
use crate::metadata::MetadataStore;

/// Result of a merge operation.
pub struct MergeResult {
    pub winner_object_id: String,
    pub losers_merged: usize,
}

/// Validate that winner and losers share the same checksum_sha256.
/// Returns a validated merge plan; the caller is responsible for actual deletion.
pub async fn merge_duplicates(
    metadata: &MetadataStore,
    winner_object_id: &str,
    loser_object_ids: &[&str],
) -> Result<MergeResult, S3Error> {
    let winner = metadata
        .get_object_by_id(winner_object_id)
        .await?
        .ok_or(S3Error::NoSuchKey)?;
    let winner_hash = winner
        .checksum_sha256
        .as_ref()
        .ok_or(S3Error::ScanPending {
            operation: "MergeDuplicates",
            files_pending: 1,
            percent_complete: 0.0,
        })?;

    for loser_id in loser_object_ids {
        let loser = metadata
            .get_object_by_id(loser_id)
            .await?
            .ok_or(S3Error::NoSuchKey)?;
        let loser_hash = loser.checksum_sha256.as_ref().ok_or(S3Error::ScanPending {
            operation: "MergeDuplicates",
            files_pending: 1,
            percent_complete: 0.0,
        })?;
        if loser_hash != winner_hash {
            return Err(S3Error::InvalidRequest(format!(
                "loser {} has different hash than winner",
                loser_id
            )));
        }
    }

    Ok(MergeResult {
        winner_object_id: winner_object_id.to_string(),
        losers_merged: loser_object_ids.len(),
    })
}
