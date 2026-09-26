use tokio::time::Instant;

use crate::net::messages::FrontendPid;

/// Connection request.
#[derive(Clone, Debug, Copy)]
pub(crate) struct Request {
    pub(crate) id: FrontendPid,
    pub(crate) created_at: Instant,
    pub(crate) read: bool,

    // Load balancer uses this to determine if primary should be allowed to read.
    // Propagated from `User.read_only` setting.
    pub(crate) read_only: bool,

    /// Read-your-writes floor: only replicas whose replay offset (bytes) reached
    /// this value may serve the read. `i64::MAX` pins the read to the primary.
    pub(crate) min_lsn: Option<i64>,
}

impl Request {
    pub(crate) fn new(id: FrontendPid, read: bool, read_only: bool) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read,
            read_only,
            min_lsn: None,
        }
    }

    pub(crate) fn unrouted(id: FrontendPid) -> Self {
        Self {
            id,
            created_at: Instant::now(),
            read: false,
            read_only: false,
            min_lsn: None,
        }
    }

    pub(crate) fn with_min_lsn(mut self, min_lsn: Option<i64>) -> Self {
        self.min_lsn = min_lsn;
        self
    }
}

impl Default for Request {
    fn default() -> Self {
        Self::unrouted(FrontendPid::new())
    }
}
