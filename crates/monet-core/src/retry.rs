//! The built-in retry policy (v2/W5): extra attempts on a 5xx response or
//! a network error, the backoff doubling per attempt.

use std::time::Duration;

use crate::error::Error;

/// How many extra attempts to make, and the first backoff.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Extra attempts beyond the first (0 = off).
    pub retries: u32,
    /// The first backoff in milliseconds; doubles per attempt.
    pub backoff_ms: u64,
}

/// How an attempt ended, as the policy sees it.
#[derive(Clone, Copy, Debug)]
pub enum AttemptOutcome<'a> {
    /// A response head with this status.
    Response(u16),
    /// The attempt failed.
    Error(&'a Error),
}

impl RetryPolicy {
    /// The backoff before the next attempt, or `None` when `outcome` is
    /// the fetch's result. `used` counts the retries already spent;
    /// `replayable` says whether the request body can be sent again.
    pub fn retry_after(
        &self,
        outcome: AttemptOutcome<'_>,
        used: u32,
        replayable: bool,
    ) -> Option<Duration> {
        // A consumed body cannot be sent again: retrying would fail to
        // rebuild the request and lose the real outcome.
        if !replayable {
            return None;
        }
        let retryable = match outcome {
            AttemptOutcome::Response(status) => status >= 500,
            // A network error — never a Cancelled abort or a Hook veto.
            AttemptOutcome::Error(error) => matches!(error, Error::Transport { .. }),
        };
        if !retryable || used >= self.retries {
            return None;
        }
        let factor = 1u64.checked_shl(used).unwrap_or(u64::MAX);
        Some(Duration::from_millis(
            self.backoff_ms.saturating_mul(factor),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: RetryPolicy = RetryPolicy {
        retries: 2,
        backoff_ms: 100,
    };

    #[test]
    fn retries_5xx_and_network_errors_with_doubling_backoff() {
        assert_eq!(
            POLICY.retry_after(AttemptOutcome::Response(503), 0, true),
            Some(Duration::from_millis(100))
        );
        let network = Error::transport("connection reset");
        assert_eq!(
            POLICY.retry_after(AttemptOutcome::Error(&network), 1, true),
            Some(Duration::from_millis(200))
        );
    }

    #[test]
    fn final_outcomes_are_not_retried() {
        assert_eq!(
            POLICY.retry_after(AttemptOutcome::Response(404), 0, true),
            None
        );
        assert_eq!(
            POLICY.retry_after(AttemptOutcome::Error(&Error::Cancelled), 0, true),
            None
        );
        assert_eq!(
            POLICY.retry_after(AttemptOutcome::Response(503), 2, true),
            None
        );
        assert_eq!(
            RetryPolicy::default().retry_after(AttemptOutcome::Response(503), 0, true),
            None
        );
    }

    /// A streamed body is consumed by the attempt that sent it. Retrying
    /// would fail to rebuild the request and replace the real 5xx or
    /// network error with that replay failure — so the outcome stands.
    #[test]
    fn a_body_that_cannot_be_replayed_is_not_retried() {
        assert_eq!(
            POLICY.retry_after(AttemptOutcome::Response(503), 0, false),
            None
        );
        let network = Error::transport("connection reset");
        assert_eq!(
            POLICY.retry_after(AttemptOutcome::Error(&network), 0, false),
            None
        );
    }
}
