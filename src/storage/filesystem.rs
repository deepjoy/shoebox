use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures::StreamExt;
use md5::{Digest, Md5};
use tokio::io::AsyncWriteExt;

use crate::config::SHOEBOX_DIR;
use crate::error::S3Error;

/// What a key points to on disk.
#[derive(Debug)]
pub enum FileContent {
    /// Regular file — stream from the handle.
    Regular(tokio::fs::File),
    /// Symlink — content is the link target path (UTF-8 lossy).
    Symlink { target: String, len: u64 },
}

/// Result of a successful write: bytes written and hex-encoded MD5 digest.
#[must_use]
pub struct WriteResult {
    pub bytes_written: u64,
    pub md5_hex: String,
}

/// Filesystem-backed storage layer for a single bucket.
///
/// All operations enforce:
/// - Path stays within the bucket root (no traversal via `..`)
/// - `.shoebox/` directory is excluded from access
/// - Intermediate symlinks are blocked (only leaf symlinks allowed)
/// - Symlinks are never followed; their target is returned as content
#[derive(Clone)]
pub struct FilesystemStorage {
    root: PathBuf,
}

impl FilesystemStorage {
    /// Create a new storage layer rooted at the given directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Return the root path of this storage.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve a key to an absolute path, ensuring it stays within the bucket
    /// and that no intermediate path component is a symlink.
    ///
    /// # Security: TOCTOU caveat
    ///
    /// The intermediate-symlink check is inherently racy: a directory could be
    /// swapped for a symlink between this check and the subsequent file
    /// operation. This is a fundamental limitation of path-based filesystem
    /// security. For a race-free solution on Linux, consider `openat2` with
    /// `RESOLVE_NO_SYMLINKS` in the future.
    pub async fn resolve_path(&self, key: &str) -> Result<PathBuf, S3Error> {
        // Reject empty keys
        if key.is_empty() {
            return Err(S3Error::InvalidArgument);
        }

        // Reject keys with path traversal components before joining
        if key.split('/').any(|seg| seg == "..") {
            return Err(S3Error::InvalidArgument);
        }

        let path = self.root.join(key);

        // Belt-and-suspenders: verify the joined path stays within root
        if !path.starts_with(&self.root) {
            return Err(S3Error::InvalidArgument);
        }

        // Exclude .shoebox directory
        if path.components().any(|c| c.as_os_str() == SHOEBOX_DIR) {
            return Err(S3Error::InvalidArgument);
        }

        // Block intermediate symlinks — walk every component *before* the
        // leaf and reject if any is a symlink.  This prevents a symlink to
        // an external directory from being used as a path prefix.
        let relative = path.strip_prefix(&self.root).unwrap();
        if let Some(parent) = relative.parent() {
            let mut check = self.root.clone();
            for component in parent.components() {
                check.push(component);
                if let Ok(meta) = tokio::fs::symlink_metadata(&check).await {
                    if meta.file_type().is_symlink() {
                        return Err(S3Error::AccessDenied);
                    }
                }
            }
        }

        Ok(path)
    }

    /// Check if a key exists as a file on disk (symlink-aware — does not follow links).
    /// Returns `false` for directories.
    pub async fn exists(&self, key: &str) -> Result<bool, S3Error> {
        let path = self.resolve_path(key).await?;
        match tokio::fs::symlink_metadata(&path).await {
            Ok(meta) => Ok(!meta.is_dir()),
            Err(_) => Ok(false),
        }
    }

    /// Open a key for streaming read.
    ///
    /// Regular files return a file handle; symlinks return their target
    /// string as content (the link is never followed).
    pub async fn get(&self, key: &str) -> Result<FileContent, S3Error> {
        let path = self.resolve_path(key).await?;

        let meta = tokio::fs::symlink_metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                S3Error::NoSuchKey
            } else {
                S3Error::InternalError
            }
        })?;

        if meta.is_dir() {
            return Err(S3Error::NoSuchKey);
        }

        if meta.file_type().is_symlink() {
            let target = tokio::fs::read_link(&path)
                .await
                .map_err(|_| S3Error::InternalError)?;
            let target_str = target.to_string_lossy().to_string();
            let len = target_str.len() as u64;
            Ok(FileContent::Symlink {
                target: target_str,
                len,
            })
        } else {
            let file = tokio::fs::File::open(&path)
                .await
                .map_err(|_| S3Error::InternalError)?;
            Ok(FileContent::Regular(file))
        }
    }

    /// Open a raw `tokio::fs::File` handle for seekable access (e.g. range requests).
    ///
    /// Performs the same path-traversal and symlink safety checks as `get()`,
    /// but always returns a `tokio::fs::File` (never symlink content).
    /// Returns `NoSuchKey` for directories and symlinks.
    pub async fn get_file_handle(&self, key: &str) -> Result<tokio::fs::File, S3Error> {
        let path = self.resolve_path(key).await?;

        let meta = tokio::fs::symlink_metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                S3Error::NoSuchKey
            } else {
                S3Error::InternalError
            }
        })?;

        if meta.is_dir() || meta.file_type().is_symlink() {
            return Err(S3Error::NoSuchKey);
        }

        tokio::fs::File::open(&path)
            .await
            .map_err(|_| S3Error::InternalError)
    }

    /// Stream request body to disk.
    ///
    /// Returns bytes written and hex-encoded MD5 digest so the caller can set
    /// `Content-Length` and `ETag` without a second pass over the data.
    ///
    /// If the target path is currently a symlink it is removed first — writes
    /// never follow symlinks.
    ///
    /// Parent directories are created automatically via `create_dir_all`.
    /// These empty directories may appear as spurious common prefixes if the
    /// bucket is subsequently listed before any objects are written into them.
    ///
    /// TODO(#7): Check available disk space before writing and reject with an
    /// appropriate error if the object would not fit.
    /// https://github.com/deepjoy/shoebox/issues/7
    pub async fn put(
        &self,
        key: &str,
        mut stream: impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
    ) -> Result<WriteResult, S3Error> {
        let path = self.resolve_path(key).await?;

        // Create parent directories
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Never write through a symlink — remove it first
        if let Ok(meta) = tokio::fs::symlink_metadata(&path).await {
            if meta.file_type().is_symlink() {
                tokio::fs::remove_file(&path).await?;
            }
        }

        let mut file = tokio::fs::File::create(&path).await?;
        let mut hasher = Md5::new();
        let mut written: u64 = 0;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| S3Error::InternalError)?;
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
            written += chunk.len() as u64;
        }

        file.sync_all().await?;

        let md5_hex = hex::encode(hasher.finalize());

        Ok(WriteResult {
            bytes_written: written,
            md5_hex,
        })
    }

    /// Delete a key from disk.  For symlinks this removes the link itself,
    /// not the target — no special handling required.
    pub async fn delete(&self, key: &str) -> Result<(), S3Error> {
        let path = self.resolve_path(key).await?;
        tokio::fs::remove_file(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                S3Error::NoSuchKey
            } else {
                S3Error::InternalError
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use tempfile::TempDir;

    fn make_storage(tmp: &TempDir) -> FilesystemStorage {
        FilesystemStorage::new(tmp.path().to_path_buf())
    }

    #[tokio::test]
    async fn test_resolve_path_valid_key() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);
        let path = storage.resolve_path("photos/cat.jpg").await.unwrap();
        assert!(path.starts_with(tmp.path()));
        assert!(path.ends_with("photos/cat.jpg"));
    }

    #[tokio::test]
    async fn test_resolve_path_traversal_blocked() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);
        assert!(storage.resolve_path("../../etc/passwd").await.is_err());
    }

    #[tokio::test]
    async fn test_resolve_path_shoebox_dir_blocked() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);
        assert!(storage.resolve_path(".shoebox/config.toml").await.is_err());
        assert!(storage.resolve_path("foo/.shoebox/bar").await.is_err());
    }

    #[tokio::test]
    async fn test_resolve_path_empty_key_blocked() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);
        assert!(storage.resolve_path("").await.is_err());
    }

    #[tokio::test]
    async fn test_put_and_get_regular_file() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        let data = b"hello, world!";
        let stream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(data))]);
        let result = storage.put("test.txt", stream).await.unwrap();
        assert_eq!(result.bytes_written, 13);
        assert!(!result.md5_hex.is_empty());

        match storage.get("test.txt").await.unwrap() {
            FileContent::Regular(mut file) => {
                use tokio::io::AsyncReadExt;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).await.unwrap();
                assert_eq!(buf, data);
            }
            FileContent::Symlink { .. } => panic!("Expected regular file"),
        }
    }

    #[tokio::test]
    async fn test_put_creates_parent_directories() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        let data = Bytes::from_static(b"nested file");
        let stream = stream::iter(vec![Ok::<_, std::io::Error>(data)]);
        let _ = storage.put("a/b/c/deep.txt", stream).await.unwrap();

        assert!(tmp.path().join("a/b/c/deep.txt").exists());
    }

    #[tokio::test]
    async fn test_delete_file() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        // Create file
        let stream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(
            b"delete me",
        ))]);
        let _ = storage.put("to-delete.txt", stream).await.unwrap();
        assert!(storage.exists("to-delete.txt").await.unwrap());

        // Delete it
        storage.delete("to-delete.txt").await.unwrap();
        assert!(!storage.exists("to-delete.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_get_directory_returns_no_such_key() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        // Create a directory (not a file)
        std::fs::create_dir_all(tmp.path().join("somedir")).unwrap();

        match storage.get("somedir").await {
            Err(S3Error::NoSuchKey) => {}
            other => panic!("Expected NoSuchKey, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_delete_nonexistent_returns_no_such_key() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        match storage.delete("nope.txt").await {
            Err(S3Error::NoSuchKey) => {}
            other => panic!("Expected NoSuchKey, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_no_such_key() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        match storage.get("nope.txt").await {
            Err(S3Error::NoSuchKey) => {}
            other => panic!("Expected NoSuchKey, got {:?}", other),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_symlink_leaf_returns_target() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        // Create a symlink
        let link_path = tmp.path().join("mylink");
        std::os::unix::fs::symlink("/some/target/path", &link_path).unwrap();

        match storage.get("mylink").await.unwrap() {
            FileContent::Symlink { target, len } => {
                assert_eq!(target, "/some/target/path");
                assert_eq!(len, target.len() as u64);
            }
            FileContent::Regular(_) => panic!("Expected symlink content"),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_intermediate_symlink_blocked() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        // Create a directory symlink
        let link_path = tmp.path().join("link-to-etc");
        std::os::unix::fs::symlink("/etc", &link_path).unwrap();

        match storage.resolve_path("link-to-etc/passwd").await {
            Err(S3Error::AccessDenied) => {}
            other => panic!("Expected AccessDenied, got {:?}", other),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_put_over_symlink_replaces_it() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        // Create a symlink
        let link_path = tmp.path().join("overwrite-me");
        std::os::unix::fs::symlink("/some/target", &link_path).unwrap();

        // Put should replace the symlink with a regular file
        let stream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(
            b"real content",
        ))]);
        let _ = storage.put("overwrite-me", stream).await.unwrap();

        let meta = std::fs::symlink_metadata(tmp.path().join("overwrite-me")).unwrap();
        assert!(!meta.file_type().is_symlink());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_exists_detects_symlink() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        let link_path = tmp.path().join("a-link");
        std::os::unix::fs::symlink("/nonexistent", &link_path).unwrap();

        // exists should return true for the symlink itself, even if target doesn't exist
        assert!(storage.exists("a-link").await.unwrap());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_delete_symlink_removes_link_not_target() {
        let tmp = TempDir::new().unwrap();
        let storage = make_storage(&tmp);

        // Create a real file
        let target = tmp.path().join("real-file");
        std::fs::write(&target, "real content").unwrap();

        // Create a symlink to it within the bucket
        let link_path = tmp.path().join("link-to-real");
        std::os::unix::fs::symlink(&target, &link_path).unwrap();

        // Delete the symlink
        storage.delete("link-to-real").await.unwrap();

        // Symlink should be gone
        assert!(!link_path.exists());
        // But the real file should still exist
        assert!(target.exists());
    }
}
