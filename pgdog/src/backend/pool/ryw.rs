//! Read-your-writes floors: when the last write completed in a scope.
//!
//! Floors are process-global so configuration reloads (which rebuild clusters
//! and pools) do not lose them. The key space is bounded by users x databases x shards.
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use pgdog_config::ReadYourWrites;

type Key = (Option<String>, String, usize);

static FLOORS: LazyLock<DashMap<Key, AtomicU64>> = LazyLock::new(DashMap::new);
/// Process start: reads before the first LSN sample after boot are held on the primary.
static BOOT_AT: LazyLock<SystemTime> = LazyLock::new(SystemTime::now);

fn nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
}

fn bump(key: Key, now: u64) {
    FLOORS
        .entry(key)
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_max(now, Ordering::Relaxed);
}

/// Record that a write just completed. Both the database and the user scope are
/// updated so switching `read_your_writes` at runtime never opens a window.
pub(crate) fn record_write(user: &str, database: &str, shard: usize) {
    let now = nanos(SystemTime::now());
    bump((None, database.to_owned(), shard), now);
    bump((Some(user.to_owned()), database.to_owned(), shard), now);
}

/// When did the last write complete in this scope? `None` if the guarantee is off.
/// Never earlier than process start.
pub(crate) fn last_write(
    scope: ReadYourWrites,
    user: &str,
    database: &str,
    shard: usize,
) -> Option<SystemTime> {
    let key = match scope {
        ReadYourWrites::Off => return None,
        ReadYourWrites::Database => (None, database.to_owned(), shard),
        ReadYourWrites::User => (Some(user.to_owned()), database.to_owned(), shard),
    };
    let recorded = FLOORS
        .get(&key)
        .map(|entry| entry.load(Ordering::Relaxed))
        .unwrap_or(0);
    let recorded = UNIX_EPOCH + Duration::from_nanos(recorded);
    Some(recorded.max(*BOOT_AT))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_off_has_no_floor() {
        record_write("u", "db_off", 0);
        assert!(last_write(ReadYourWrites::Off, "u", "db_off", 0).is_none());
    }

    #[test]
    fn test_floor_never_before_boot() {
        let floor = last_write(ReadYourWrites::Database, "u", "db_unwritten", 0).unwrap();
        assert_eq!(floor, *BOOT_AT);
    }

    #[test]
    fn test_write_raises_both_scopes_and_is_monotonic() {
        let before = SystemTime::now();
        record_write("alice", "db_w", 2);
        let db = last_write(ReadYourWrites::Database, "bob", "db_w", 2).unwrap();
        let user = last_write(ReadYourWrites::User, "alice", "db_w", 2).unwrap();
        let other_user = last_write(ReadYourWrites::User, "bob", "db_w", 2).unwrap();
        assert!(db >= before && user >= before);
        assert_eq!(other_user, *BOOT_AT);
        assert_eq!(
            last_write(ReadYourWrites::Database, "bob", "db_w", 1).unwrap(),
            *BOOT_AT
        );
        record_write("alice", "db_w", 2);
        let again = last_write(ReadYourWrites::Database, "x", "db_w", 2).unwrap();
        assert!(again >= db);
    }
}
