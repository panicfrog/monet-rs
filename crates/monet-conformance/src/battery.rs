//! The battery runner: wraps panic-based checks with `catch_unwind` so a
//! driver that cannot use the test harness — an Android app showing the
//! report on screen, a CI job summarizing across transports — gets
//! per-check outcomes instead of a first-failure abort.
//!
//! The checks themselves stay plain panicking functions (they run
//! unchanged inside `#[test]`s on the host); the battery is a lens over
//! them, not a reimplementation.

use std::any::Any;
use std::panic::AssertUnwindSafe;

/// One check's outcome.
#[derive(Debug, Clone)]
pub struct CheckReport {
    /// The check's name.
    pub name: &'static str,
    /// The panic message, when the check failed.
    pub failure: Option<String>,
}

impl CheckReport {
    /// `true` when the check passed.
    pub fn passed(&self) -> bool {
        self.failure.is_none()
    }

    /// `true` when the check is a documented skip (a manual-verdict item:
    /// the check cannot run in this environment by design, with the
    /// reason in the message). Convention: the failure message starts
    /// with `skipped:`.
    pub fn skipped(&self) -> bool {
        self.failure
            .as_ref()
            .is_some_and(|message| message.starts_with("skipped:"))
    }
}

/// Records a documented skip — a check that cannot run in this
/// environment (see [`CheckReport::skipped`]).
pub fn skip(name: &'static str, reason: &str) -> CheckReport {
    CheckReport {
        name,
        failure: Some(format!("skipped: {reason}")),
    }
}

/// Runs `body`, converting a panic into a failed [`CheckReport`].
pub fn check<F>(name: &'static str, body: F) -> CheckReport
where
    F: FnOnce(),
{
    let failure = std::panic::catch_unwind(AssertUnwindSafe(body))
        .err()
        .map(panic_message);
    CheckReport { name, failure }
}

/// Renders a report set, one line per check:
/// `name: OK`, `name: SKIP — reason`, or `name: FAILED — message`.
pub fn format(reports: &[CheckReport]) -> String {
    let mut out = String::new();
    for report in reports {
        match &report.failure {
            None => out.push_str(&format!("{}: OK\n", report.name)),
            Some(message) if report.skipped() => {
                out.push_str(&format!("{}: SKIP — {message}\n", report.name));
            }
            Some(message) => {
                out.push_str(&format!("{}: FAILED — {message}\n", report.name));
            }
        }
    }
    let passed = reports.iter().filter(|r| r.passed()).count();
    let skipped = reports.iter().filter(|r| r.skipped()).count();
    out.push_str(&format!("{passed}/{} passed", reports.len() - skipped));
    if skipped > 0 {
        out.push_str(&format!(" ({skipped} skipped)"));
    }
    out
}

fn panic_message(error: Box<dyn Any + Send>) -> String {
    if let Some(message) = error.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = error.downcast_ref::<String>() {
        message.clone()
    } else {
        "check panicked".to_string()
    }
}
