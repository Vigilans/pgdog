//! Read-your-writes: where each scope's latest write sits in the primary's WAL.
//!
//! When a write commits, its scope's reads stay on the primary while the write's
//! WAL position is fetched in the background; once known, replicas that replayed
//! it may serve them. A position that could not be fetched is unknown: reads stay
//! on the primary until a later fetch covers it.
//!
//! State is process-global so configuration reloads (which rebuild clusters and
//! pools) keep it. The key space is bounded by users x databases x shards.
use std::sync::LazyLock;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use pgdog_config::ReadYourWrites;
use pgdog_postgres_types::Format;
use tracing::warn;

use super::{Pool, Request, Shard};
use crate::{net::DataRow, tasks, util::safe_timeout};

type Key = (Option<String>, String, usize);

const TASK: &str = "read your writes position";

/// WAL insert location, in bytes: past every record inserted so far, including
/// commits made with `synchronous_commit = off` that are not written out yet.
const POSITION: &str = "SELECT pg_current_wal_insert_lsn() - '0/0'::pg_lsn";

struct Entry {
    /// Highest known write position.
    lsn: AtomicI64,
    /// Committed writes whose position is still being fetched.
    pending: AtomicU32,
    /// 0 when `lsn` covers every write; otherwise the moment (see [`moment`])
    /// since which a write's position is unknown. It only grows, so a fetch
    /// started before a later failure cannot clear that failure.
    unknown: AtomicU64,
}

static STATE: LazyLock<DashMap<Key, Entry>> = LazyLock::new(DashMap::new);
/// Origin of moments: the process's first read-your-writes activity.
static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);
/// Last moment handed out. Starts at 1, the "unknown since the start" of a new scope.
static LAST: AtomicU64 = AtomicU64::new(1);

/// Nanoseconds from [`EPOCH`] to `at` (0 before it).
fn nanos(at: Instant) -> u64 {
    duration_nanos(at.saturating_duration_since(*EPOCH))
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Now, as a moment later than every moment handed out before.
fn moment() -> u64 {
    let now = nanos(Instant::now());
    let mut last = LAST.load(Ordering::Acquire);
    loop {
        let next = now.max(last + 1);
        match LAST.compare_exchange_weak(last, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return next,
            Err(actual) => last = actual,
        }
    }
}

fn with_entry<T>(key: &Key, f: impl FnOnce(&Entry) -> T) -> T {
    if let Some(entry) = STATE.get(key) {
        return f(&entry);
    }
    f(&STATE.entry(key.clone()).or_insert_with(Entry::new))
}

impl Entry {
    /// Nothing recorded yet: unknown since the start.
    fn new() -> Self {
        Self {
            lsn: AtomicI64::new(0),
            pending: AtomicU32::new(0),
            unknown: AtomicU64::new(1),
        }
    }

    /// The minimum replay offset for a read (`i64::MAX`: primary only), and
    /// whether this read should fetch an unknown position.
    fn floor(&self, primary: Option<&Pool>) -> (i64, bool) {
        if self.pending.load(Ordering::Acquire) > 0 {
            return (i64::MAX, false);
        }
        let unknown = self.unknown.load(Ordering::Acquire);
        if unknown == 0 {
            return (self.lsn.load(Ordering::Acquire), false);
        }
        let Some(primary) = primary else {
            return (i64::MAX, false);
        };
        let config = primary.config();
        let interval = duration_nanos(config.lsn_check_interval);
        if config.lsn_checks_enabled() {
            // A primary sample queried more than an interval after the position
            // became unknown saw every write before it. The interval also covers
            // commits made with `synchronous_commit = off`: the sampled WAL write
            // location passes them within 3 x `wal_writer_delay`.
            let sample = primary.lsn_stats();
            let covers = sample.valid()
                && !sample.replica
                && sample
                    .queried_at
                    .is_some_and(|at| nanos(at) > unknown.saturating_add(interval));
            if covers {
                self.lsn.fetch_max(sample.offset_bytes, Ordering::AcqRel);
                if self
                    .unknown
                    .compare_exchange(unknown, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return (self.lsn.load(Ordering::Acquire), false);
                }
            }
            return (i64::MAX, false);
        }
        // Unknown for an interval: this read, served by the primary, fetches the
        // position. Of concurrent reads, only the one that moves `unknown` does.
        let due = nanos(Instant::now()).saturating_sub(unknown) >= interval;
        let fetch = due
            && self
                .unknown
                .compare_exchange(unknown, moment(), Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        (i64::MAX, fetch)
    }
}

/// One position fetch. Dropping it unresolved (failure, timeout, panic,
/// shutdown) marks the position unknown.
struct Flight {
    keys: Vec<Key>,
    /// Counted in `pending`: the fetch for a committed write.
    counted: bool,
    /// `unknown` of each key right before the query was sent.
    seen: Vec<u64>,
    resolved: bool,
}

impl Flight {
    fn start(keys: Vec<Key>, counted: bool) -> Self {
        if counted {
            for key in &keys {
                with_entry(key, |entry| entry.pending.fetch_add(1, Ordering::AcqRel));
            }
        }
        Self {
            keys,
            counted,
            seen: vec![],
            resolved: false,
        }
    }

    /// Whatever is unknown now, a position fetched afterwards covers.
    fn arm(&mut self) {
        self.seen = self
            .keys
            .iter()
            .map(|key| with_entry(key, |entry| entry.unknown.load(Ordering::Acquire)))
            .collect();
    }

    fn resolve(&mut self, lsn: i64) {
        for (key, seen) in self.keys.iter().zip(&self.seen) {
            with_entry(key, |entry| {
                entry.lsn.fetch_max(lsn, Ordering::AcqRel);
                if *seen != 0 {
                    let _ = entry.unknown.compare_exchange(
                        *seen,
                        0,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }
            });
        }
        self.resolved = true;
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        let failed_at = (!self.resolved).then(moment);
        for key in &self.keys {
            with_entry(key, |entry| {
                if let Some(failed_at) = failed_at {
                    entry.unknown.fetch_max(failed_at, Ordering::AcqRel);
                }
                if self.counted {
                    entry.pending.fetch_sub(1, Ordering::Release);
                }
            });
        }
    }
}

/// A write committed on `shard`: its scopes' reads stay on the primary until
/// its position is fetched. Both scopes are recorded so switching
/// `read_your_writes` at runtime never opens a window.
pub(crate) fn record_write(shard: &Shard, user: &str, database: &str, number: usize) {
    let keys = vec![
        (None, database.to_owned(), number),
        (Some(user.to_owned()), database.to_owned(), number),
    ];
    tasks::spawn(TASK, fetch(shard.clone(), Flight::start(keys, true)));
}

/// Minimum replay offset a replica needs to serve a read in this scope.
/// `None`: no constraint. `Some(i64::MAX)`: primary only.
pub(crate) fn min_lsn(
    scope: ReadYourWrites,
    user: &str,
    database: &str,
    number: usize,
    shard: &Shard,
) -> Option<i64> {
    let key = match scope {
        ReadYourWrites::Off => return None,
        ReadYourWrites::Database => (None, database.to_owned(), number),
        ReadYourWrites::User => (Some(user.to_owned()), database.to_owned(), number),
    };
    let (floor, fetch_now) = with_entry(&key, |entry| entry.floor(shard.primary_pool()));
    if fetch_now {
        tasks::spawn(TASK, fetch(shard.clone(), Flight::start(vec![key], false)));
    }
    Some(floor)
}

async fn fetch(shard: Shard, mut flight: Flight) {
    flight.arm();
    match position(&shard).await {
        Some(lsn) => flight.resolve(lsn),
        None => warn!(
            r#"read-your-writes: no WAL position for database "{}" shard {}, its reads stay on the primary until one is known"#,
            flight.keys[0].1, flight.keys[0].2
        ),
    }
}

/// The primary's WAL insert position, within `lsn_check_timeout`.
async fn position(shard: &Shard) -> Option<i64> {
    let timeout = shard.primary_pool()?.config().lsn_check_timeout;
    let rows = safe_timeout(timeout, async {
        let mut conn = shard.primary(&Request::default()).await.ok()?;
        conn.fetch_all::<DataRow>(POSITION).await.ok()
    })
    .await
    .ok()??;
    rows.first()?.get(0, Format::Text)
}

#[cfg(test)]
mod test {
    use pgdog_config::MAX_DURATION;
    use pgdog_stats::{Lsn, LsnStats as StatsLsnStats};

    use super::super::{Address, Config, LsnStats, PoolConfig, ShardConfig};
    use super::*;

    fn key(database: &str) -> Key {
        (None, database.to_owned(), 0)
    }

    fn pool(lsn_check_delay: Duration, lsn_check_interval: Duration) -> Pool {
        Pool::new(&PoolConfig {
            address: Address::new_test(),
            config: Config {
                lsn_check_delay,
                lsn_check_interval,
                ..Config::default()
            },
        })
    }

    #[test]
    fn test_moments_only_grow() {
        let first = moment();
        let second = moment();
        assert!(first >= 2);
        assert!(second > first);
    }

    #[test]
    fn test_new_scope_reads_primary() {
        assert_eq!(Entry::new().floor(None), (i64::MAX, false));
    }

    #[test]
    fn test_pending_write_holds_primary() {
        let entry = Entry::new();
        entry.unknown.store(0, Ordering::Release);
        entry.lsn.store(100, Ordering::Release);
        entry.pending.store(1, Ordering::Release);
        assert_eq!(entry.floor(None), (i64::MAX, false));
        entry.pending.store(0, Ordering::Release);
        assert_eq!(entry.floor(None), (100, false));
    }

    #[test]
    fn test_fetched_position_resolves_both_scopes() {
        let keys = vec![
            (None, "db_resolve".to_owned(), 0),
            (Some("u".to_owned()), "db_resolve".to_owned(), 0),
        ];
        let mut flight = Flight::start(keys.clone(), true);
        flight.arm();
        for key in &keys {
            assert_eq!(
                with_entry(key, |entry| entry.floor(None)),
                (i64::MAX, false)
            );
        }
        flight.resolve(500);
        drop(flight);
        for key in &keys {
            assert_eq!(with_entry(key, |entry| entry.floor(None)), (500, false));
        }
    }

    #[test]
    fn test_failed_fetch_marks_position_unknown() {
        let keys = vec![key("db_failed")];
        let mut flight = Flight::start(keys.clone(), true);
        flight.arm();
        drop(flight);
        with_entry(&keys[0], |entry| {
            assert_eq!(entry.pending.load(Ordering::Acquire), 0);
            assert!(entry.unknown.load(Ordering::Acquire) >= 2);
        });
    }

    #[test]
    fn test_late_success_keeps_newer_failure() {
        let keys = vec![key("db_race")];
        let mut first = Flight::start(keys.clone(), true);
        first.arm();
        let mut second = Flight::start(keys.clone(), true);
        second.arm();
        drop(second);
        let failed_at = with_entry(&keys[0], |entry| entry.unknown.load(Ordering::Acquire));

        first.resolve(700);
        drop(first);
        with_entry(&keys[0], |entry| {
            assert_eq!(entry.unknown.load(Ordering::Acquire), failed_at);
            assert_eq!(entry.lsn.load(Ordering::Acquire), 700);
            assert_eq!(entry.floor(None), (i64::MAX, false));
        });
    }

    #[test]
    fn test_unknown_read_fetches_once_per_interval() {
        let primary = pool(MAX_DURATION, Duration::from_millis(50));
        let entry = Entry::new();
        entry.unknown.store(moment(), Ordering::Release);
        assert_eq!(entry.floor(Some(&primary)), (i64::MAX, false));

        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(entry.floor(Some(&primary)), (i64::MAX, true));
        assert_eq!(entry.floor(Some(&primary)), (i64::MAX, false));
    }

    // Needs the local Postgres (Task 14 Step 1).
    #[tokio::test]
    async fn test_write_position_is_fetched_from_the_primary() {
        crate::logger();
        let shard = Shard::new(ShardConfig {
            primary: Some(&PoolConfig {
                address: Address::new_test(),
                config: Config::default(),
            }),
            ..Default::default()
        });
        shard.launch();

        record_write(&shard, "u", "db_fetch", 0);
        let lsn = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match min_lsn(ReadYourWrites::Database, "u", "db_fetch", 0, &shard) {
                    Some(lsn) if lsn != i64::MAX => return lsn,
                    _ => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .expect("the write position should be fetched");

        assert!(lsn > 0);
        assert_eq!(
            min_lsn(ReadYourWrites::User, "u", "db_fetch", 0, &shard),
            Some(lsn)
        );
        assert_eq!(
            min_lsn(ReadYourWrites::Off, "u", "db_fetch", 0, &shard),
            None
        );
        shard.shutdown();
    }

    fn set_primary_sample(pool: &Pool, offset: i64, queried_at: Instant) {
        let mut sample: LsnStats = StatsLsnStats {
            replica: false,
            lsn: Lsn::from_i64(offset),
            offset_bytes: offset,
            ..Default::default()
        }
        .into();
        sample.queried_at = Some(queried_at);
        *pool.inner().lsn_stats.write() = sample;
    }

    #[test]
    fn test_primary_sample_after_failure_resolves() {
        let primary = pool(Duration::ZERO, Duration::from_millis(10));
        let entry = Entry::new();
        entry.unknown.store(moment(), Ordering::Release);

        // Queried before the failure: it may predate the write.
        set_primary_sample(&primary, 900, Instant::now() - Duration::from_millis(100));
        assert_eq!(entry.floor(Some(&primary)), (i64::MAX, false));

        // Queried more than an interval after it: it saw the write.
        std::thread::sleep(Duration::from_millis(20));
        set_primary_sample(&primary, 900, Instant::now());
        assert_eq!(entry.floor(Some(&primary)), (900, false));
        assert_eq!(entry.unknown.load(Ordering::Acquire), 0);
    }
}
