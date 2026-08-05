//! Durable per-port seq floors for this node's journal shards (issue #351).
//!
//! A journal shard is process memory with no durable state, which is Ch.7's volatility
//! decision and stays true — entries are test-run-scoped and are not persisted here.
//! But the *counter* is a different thing from the entries it numbers. `node_id` is
//! stable across restarts (see [`crate::raft::identity`]), so a shard whose `seq`
//! restarted at 1 hands out `(node_id, seq)` pairs the fleet has already seen: the
//! survivors still hold pre-crash entries under those exact keys in their replica
//! caches, and every walker's cursor is a position indexed by them. The collision is
//! silent in both directions — merge dedup drops one of the two entries by identity,
//! and the cursor filter withholds every reborn seq below a surviving position.
//!
//! So the fix is to persist the counter's *high-water*, not the entries. What this
//! file guarantees is one invariant:
//!
//! > No seq is ever handed out that is greater than the floor already durable on disk.
//!
//! Restart then resumes at the persisted floor, which is at or above every seq the
//! previous boot could possibly have used, so old cursor positions sit strictly below
//! all new seqs and identity holds.
//!
//! # Why this is not a hot-path write
//!
//! `09-durability-failure.md` rejects persisting journal state because "each would cost
//! hot-path writes". A naive floor that fsynced per append would deserve that
//! objection. This one allocates in blocks: crossing the floor persists
//! `seq + `[`SEQ_FLOOR_SLACK`], so the cost is one fsync per 2^20 appends per port and
//! the recording path is untouched in between.
//!
//! The unused span `(last_handed, floor]` is deliberate and harmless. The cursor
//! contract promises no missed **entries**, never dense seqs, and every comparison in
//! the merge, eviction and cursor paths is an inequality.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

/// Filename holding the persisted floors, relative to the cluster state directory.
const SEQ_FLOOR_FILE: &str = "journal-seq-floors";

/// First line of the floor file.
///
/// It carries no format-evolution weight yet — it exists so the file **fails closed**
/// when empty. Without it a 0-byte file (an operator `truncate`, a partial backup
/// restore, an image that pre-creates the path) parses as "no floors", every port reads
/// 0, and the seq reuse this module prevents comes back silently. A present file that
/// does not open with this line is corrupt, and corrupt refuses to start.
///
/// This is also what makes the `NodeIdentity` comparison below honest: `identity.rs`
/// parses an empty node-id file as `InvalidData` rather than "absent", and this is the
/// equivalent for a map, which has no natural non-empty shape to fail on.
const SEQ_FLOOR_HEADER: &str = "v1";

/// How far past the seq being handed out a persist reserves.
///
/// This is the amortization constant: one synchronous write per this many appends per
/// port. At 2^20, a shard recording continuously at 10k requests/second persists about
/// once every two minutes (2^20 / 10^4 ≈ 105 s), while a normal test run persists once
/// at first append and never again. Raising it costs nothing but a wider unused seq
/// span; lowering it buys nothing, because the span is never observed.
const SEQ_FLOOR_SLACK: u64 = 1 << 20;

/// Durable per-port seq floors, or an ephemeral stand-in when there is no state
/// directory to write to.
///
/// The ephemeral form is what embedders and unit tests get from
/// [`crate::stores::ClusterJournal::new`]: it reports every floor as 0 and never
/// writes, which is exactly the pre-#351 behaviour. That is correct rather than
/// merely convenient — a journal with nowhere to persist cannot make the guarantee,
/// and pretending otherwise would be worse than not making it.
#[derive(Debug)]
pub(crate) struct SeqFloors {
    /// `None` = ephemeral: no file, no durability, every floor reads 0.
    path: Option<PathBuf>,
    /// Every known floor, including ports this boot has not touched. Held whole
    /// because a persist rewrites the entire file, and dropping an untouched port's
    /// floor would re-open the collision for that port on the next restart.
    floors: Mutex<BTreeMap<u16, u64>>,
}

impl SeqFloors {
    /// Floors that are never persisted and always read 0.
    pub(crate) fn ephemeral() -> Self {
        Self {
            path: None,
            floors: Mutex::new(BTreeMap::new()),
        }
    }

    /// Load the floors persisted under `dir`, or start empty if the file is absent.
    ///
    /// A file that exists but does not parse is a corrupt state directory, not an
    /// absent one, and is surfaced as an error so the node refuses to start. This
    /// mirrors [`crate::raft::identity::NodeIdentity::load`] deliberately: there is no
    /// safe recovery. Starting from 0 over a damaged floor file is precisely the seq
    /// reuse this module exists to prevent, and no other value can be inferred, since
    /// the entries that would reveal the true high-water are the volatile thing that
    /// did not survive.
    pub(crate) fn load(dir: &Path) -> io::Result<Self> {
        let path = dir.join(SEQ_FLOOR_FILE);
        let floors = match std::fs::read_to_string(&path) {
            Ok(contents) => Self::parse(&contents, &path)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e),
        };
        Ok(Self {
            path: Some(path),
            floors: Mutex::new(floors),
        })
    }

    fn parse(contents: &str, path: &Path) -> io::Result<BTreeMap<u16, u64>> {
        let corrupt = |detail: String| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "corrupt journal seq floor file {}: {detail}",
                    path.display()
                ),
            )
        };
        let mut lines = contents.lines();
        // Checked before anything else, so an empty or truncated file is rejected rather
        // than read as "no floors" -- see SEQ_FLOOR_HEADER.
        match lines.next().map(str::trim) {
            Some(SEQ_FLOOR_HEADER) => {}
            Some(other) => {
                return Err(corrupt(format!(
                    "expected header `{SEQ_FLOOR_HEADER}`, found `{other}`"
                )));
            }
            None => return Err(corrupt("file is empty".to_string())),
        }
        let mut floors = BTreeMap::new();
        for (lineno, line) in lines.enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let (port, floor) = line
                .split_once('=')
                .ok_or_else(|| corrupt(format!("line {} is not `port=floor`", lineno + 2)))?;
            let port = port
                .trim()
                .parse::<u16>()
                .map_err(|e| corrupt(format!("line {}: bad port: {e}", lineno + 2)))?;
            let floor = floor
                .trim()
                .parse::<u64>()
                .map_err(|e| corrupt(format!("line {}: bad floor: {e}", lineno + 2)))?;
            floors.insert(port, floor);
        }
        Ok(floors)
    }

    /// The durable floor for `port`; 0 when this port has never been persisted.
    pub(crate) fn floor(&self, port: u16) -> u64 {
        self.floors.lock().get(&port).copied().unwrap_or(0)
    }

    /// Reserve durably through at least `seq`, returning the new floor.
    ///
    /// Persists `seq + `[`SEQ_FLOOR_SLACK`] so the next 2^20 appends need no write.
    /// Returns the reserved floor even when the write failed, because the caller has
    /// already decided to hand `seq` out and needs an in-memory floor to advance to;
    /// the durability of that reservation is what was lost, and the error is the
    /// caller's to report.
    pub(crate) fn reserve_through(&self, port: u16, seq: u64) -> (u64, io::Result<()>) {
        let reserved = seq.saturating_add(SEQ_FLOOR_SLACK);
        let mut floors = self.floors.lock();
        // Re-check under the lock: two ports crossing at once serialize here, and a
        // racing caller for the same port may already have reserved past `seq`.
        if floors.get(&port).copied().unwrap_or(0) >= seq {
            return (floors.get(&port).copied().unwrap_or(0), Ok(()));
        }
        floors.insert(port, reserved);
        let Some(path) = self.path.as_deref() else {
            return (reserved, Ok(()));
        };
        let result = Self::persist(path, &floors);
        (reserved, result)
    }

    /// Write the whole map with the crash-safe pattern `NodeIdentity::load_or_mint`
    /// established: temp file, fsync it, rename over the target, fsync the directory.
    ///
    /// The rename is what makes a crash mid-write leave either the old floors or the
    /// new ones and never a truncated file — which matters more here than it does for
    /// the node id, because a half-written floor file parses as corrupt and, per
    /// [`Self::load`], stops the node from starting at all.
    fn persist(path: &Path, floors: &BTreeMap<u16, u64>) -> io::Result<()> {
        use std::fmt::Write as _;
        let mut body = String::with_capacity(floors.len() * 16 + 4);
        body.push_str(SEQ_FLOOR_HEADER);
        body.push('\n');
        for (port, floor) in floors {
            // Writes into the pre-sized buffer; `push_str(&format!(..))` would allocate
            // and drop a String per line for nothing. Cannot fail — the `fmt::Write` impl
            // for String is infallible — so the Result is discarded deliberately rather
            // than propagated into this function's io::Result.
            let _ = writeln!(body, "{port}={floor}");
        }
        let tmp = path.with_extension("tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut file, body.as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        let dir = path
            .parent()
            .ok_or_else(|| io::Error::other("seq floor path has no parent directory"))?;
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn an_absent_file_reads_every_floor_as_zero() {
        let dir = TempDir::new().expect("tempdir");
        let floors = SeqFloors::load(dir.path()).expect("load");
        assert_eq!(floors.floor(8080), 0);
        assert_eq!(floors.floor(9090), 0);
    }

    #[test]
    fn a_reservation_round_trips_through_the_file() {
        let dir = TempDir::new().expect("tempdir");
        let floors = SeqFloors::load(dir.path()).expect("load");
        let (reserved, wrote) = floors.reserve_through(8080, 1);
        wrote.expect("persist");
        assert_eq!(reserved, 1 + SEQ_FLOOR_SLACK);

        let reloaded = SeqFloors::load(dir.path()).expect("reload");
        assert_eq!(reloaded.floor(8080), 1 + SEQ_FLOOR_SLACK);
        // A port that was never reserved stays 0 across the round trip.
        assert_eq!(reloaded.floor(9090), 0);
    }

    #[test]
    fn reserving_one_port_preserves_every_other_ports_floor() {
        // The whole-map rewrite is the point: dropping an untouched port's floor would
        // re-open the seq collision for that port on the next restart.
        let dir = TempDir::new().expect("tempdir");
        let floors = SeqFloors::load(dir.path()).expect("load");
        floors.reserve_through(8080, 5).1.expect("persist 8080");
        floors.reserve_through(9090, 7).1.expect("persist 9090");

        let reloaded = SeqFloors::load(dir.path()).expect("reload");
        assert_eq!(reloaded.floor(8080), 5 + SEQ_FLOOR_SLACK);
        assert_eq!(reloaded.floor(9090), 7 + SEQ_FLOOR_SLACK);
    }

    #[test]
    fn a_reservation_already_covered_does_not_rewrite() {
        let dir = TempDir::new().expect("tempdir");
        let floors = SeqFloors::load(dir.path()).expect("load");
        floors.reserve_through(8080, 1).1.expect("persist");
        let first = floors.floor(8080);

        // Well inside the reserved block: no new reservation, same floor.
        let (reserved, wrote) = floors.reserve_through(8080, 2);
        wrote.expect("no write needed");
        assert_eq!(reserved, first);
        assert_eq!(floors.floor(8080), first);
    }

    #[test]
    fn an_ephemeral_store_reads_zero_and_never_writes() {
        let floors = SeqFloors::ephemeral();
        assert_eq!(floors.floor(8080), 0);
        let (reserved, wrote) = floors.reserve_through(8080, 1);
        wrote.expect("ephemeral reserve cannot fail");
        // It still tracks in memory, so a single boot's allocation stays monotone.
        assert_eq!(reserved, 1 + SEQ_FLOOR_SLACK);
        assert_eq!(floors.floor(8080), 1 + SEQ_FLOOR_SLACK);
    }

    #[test]
    fn a_corrupt_file_refuses_to_load() {
        // Refusing beats defaulting to 0: 0 is exactly the seq reuse this module
        // exists to prevent, and nothing on disk can reveal the true high-water.
        let dir = TempDir::new().expect("tempdir");
        std::fs::write(dir.path().join(SEQ_FLOOR_FILE), "v1\n8080=not-a-number\n").expect("write");
        let err = SeqFloors::load(dir.path()).expect_err("corrupt file must not load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        std::fs::write(dir.path().join(SEQ_FLOOR_FILE), "v1\nno-equals-sign\n").expect("write");
        let err = SeqFloors::load(dir.path()).expect_err("corrupt file must not load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn blank_lines_are_tolerated() {
        let dir = TempDir::new().expect("tempdir");
        std::fs::write(
            dir.path().join(SEQ_FLOOR_FILE),
            "v1\n8080=12\n\n9090=34\n\n",
        )
        .expect("write");
        let floors = SeqFloors::load(dir.path()).expect("load");
        assert_eq!(floors.floor(8080), 12);
        assert_eq!(floors.floor(9090), 34);
    }

    #[test]
    fn a_reservation_near_the_top_of_the_range_saturates_rather_than_wrapping() {
        // Wrapping would hand out a floor BELOW the seq being reserved, which is the
        // one arithmetic outcome that would silently reintroduce the collision.
        let floors = SeqFloors::ephemeral();
        let (reserved, _) = floors.reserve_through(8080, u64::MAX - 1);
        assert_eq!(reserved, u64::MAX);
        assert!(reserved >= u64::MAX - 1);
    }

    #[test]
    fn an_empty_but_present_file_refuses_to_load() {
        // The failure this header exists for. A 0-byte file used to parse as "no floors",
        // so every port read 0 -- silently reinstating the seq reuse the module prevents.
        // An ABSENT file is still a legitimate first boot; a present-but-empty one is not.
        let dir = TempDir::new().expect("tempdir");
        std::fs::write(dir.path().join(SEQ_FLOOR_FILE), "").expect("write");
        let err = SeqFloors::load(dir.path()).expect_err("an empty file must not load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_file_with_an_unknown_header_refuses_to_load() {
        let dir = TempDir::new().expect("tempdir");
        std::fs::write(dir.path().join(SEQ_FLOOR_FILE), "v2\n8080=12\n").expect("write");
        let err = SeqFloors::load(dir.path()).expect_err("an unknown header must not load");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_written_file_carries_the_header() {
        // Round-trip is already covered; this pins the on-disk shape itself, so the
        // header cannot be dropped from `persist` while `parse` still demands it (which
        // would brick every restart rather than fail a test).
        let dir = TempDir::new().expect("tempdir");
        let floors = SeqFloors::load(dir.path()).expect("load");
        floors.reserve_through(8080, 1).1.expect("persist");
        let body = std::fs::read_to_string(dir.path().join(SEQ_FLOOR_FILE)).expect("read");
        assert!(
            body.starts_with("v1\n"),
            "floor file must open with the version header, got {body:?}"
        );
    }
}
