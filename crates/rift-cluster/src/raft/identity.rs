//! Node identity: a `u64` Raft node id, minted once and persisted in the state
//! directory so a restart rejoins as the *same* node rather than a new one.
//!
//! Per ADR-001 the id replaces the v2 `name@addr#incarnation` scheme. Every
//! node mints its own id at first start — founder and joiner alike — from
//! `--cluster-node-name` when set (so a redeployed pod with the same name and a
//! wiped state dir returns as the same node) and from the clock otherwise; the
//! join request carries it to the leader as-is. Once persisted it is
//! authoritative for every later start.

use std::path::{Path, PathBuf};

/// Filename holding the persisted node id, relative to the state directory.
const NODE_ID_FILE: &str = "node-id";

/// A node's persisted Raft identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeIdentity {
    node_id: u64,
}

impl NodeIdentity {
    /// The persisted node id.
    #[must_use]
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    fn path(dir: &Path) -> PathBuf {
        dir.join(NODE_ID_FILE)
    }

    /// Return the identity already persisted under `dir`, or `None` if this node
    /// has never been given one.
    ///
    /// A file that exists but does not hold a single valid `u64` is a corrupt
    /// state directory, not an absent identity — it is surfaced as an error so a
    /// node refuses to start rather than silently minting a second identity over
    /// a damaged one.
    pub fn load(dir: &Path) -> std::io::Result<Option<Self>> {
        let path = Self::path(dir);
        let contents = match std::fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let node_id = contents.trim().parse::<u64>().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("corrupt node-id file {}: {e}", path.display()),
            )
        })?;
        Ok(Some(Self { node_id }))
    }

    /// Load the persisted identity under `dir`, or mint `proposed` and persist it
    /// if none exists yet.
    ///
    /// Once an id is persisted it is authoritative: a later call with a different
    /// `proposed` still returns the persisted id and never overwrites it, so a
    /// restart keeps the node's identity stable.
    pub fn load_or_mint(dir: &Path, proposed: u64) -> std::io::Result<Self> {
        if let Some(existing) = Self::load(dir)? {
            return Ok(existing);
        }
        std::fs::create_dir_all(dir)?;
        // Write to a temp file and rename so a crash mid-write cannot leave a
        // half-written id that `load` would reject as corrupt. fsync the temp file
        // before the rename and the directory after it, so a crash in that window
        // cannot lose the id while the Raft log in the same dir still records this
        // node as a voter — which would let a restart re-mint a *different* id that
        // isn't in its own persisted membership (split identity).
        let tmp = Self::path(dir).with_extension("tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut file, proposed.to_string().as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, Self::path(dir))?;
        // fsync the directory so the rename itself is durable. A directory that
        // cannot be opened/synced (e.g. on a filesystem that disallows it) is a
        // real failure here, not something to swallow.
        std::fs::File::open(dir)?.sync_all()?;
        Ok(Self { node_id: proposed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn mints_and_persists_when_absent() {
        let dir = TempDir::new().expect("tempdir");
        assert_eq!(NodeIdentity::load(dir.path()).unwrap(), None);
        let id = NodeIdentity::load_or_mint(dir.path(), 7).unwrap();
        assert_eq!(id.node_id(), 7);
        assert!(NodeIdentity::path(dir.path()).exists());
    }

    #[test]
    fn identity_persists_across_restart() {
        let dir = TempDir::new().expect("tempdir");
        let first = NodeIdentity::load_or_mint(dir.path(), 7).unwrap();
        // A second call, simulating a restart, with a *different* proposed id must
        // return the already-persisted id — never re-mint.
        let second = NodeIdentity::load_or_mint(dir.path(), 99).unwrap();
        assert_eq!(first.node_id(), 7);
        assert_eq!(second.node_id(), 7);
    }

    #[test]
    fn corrupt_file_is_an_error_not_a_fresh_mint() {
        let dir = TempDir::new().expect("tempdir");
        std::fs::write(NodeIdentity::path(dir.path()), "not-a-number").unwrap();
        assert!(NodeIdentity::load(dir.path()).is_err());
        assert!(NodeIdentity::load_or_mint(dir.path(), 1).is_err());
    }
}
