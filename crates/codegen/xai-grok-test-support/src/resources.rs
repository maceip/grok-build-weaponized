//! Generic OS resource snapshots for soak tests. Linux reads `/proc`; macOS
//! uses the native libproc interface. Unsupported platforms return `None` for
//! metrics that cannot be sampled without external tools.

/// RSS (bytes), live threads, and open fds sampled together. `None` marks a
/// metric the platform can't report.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResourceSnapshot {
    pub rss: Option<usize>,
    pub threads: Option<usize>,
    pub fds: Option<usize>,
}

/// Saturating per-field growth of one [`ResourceSnapshot`] over an earlier
/// baseline. A distinct type from a snapshot so a delta can't be mistaken for
/// an absolute sample. `None` marks a field either side couldn't report.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResourceGrowth {
    pub rss: Option<usize>,
    pub threads: Option<usize>,
    pub fds: Option<usize>,
}

impl ResourceSnapshot {
    pub fn capture() -> Self {
        Self {
            rss: rss_bytes(),
            threads: thread_count(),
            fds: fd_count(),
        }
    }

    /// RSS only, skipping the thread and fd probes. For hot sampling loops that
    /// use just `rss`: on Linux this avoids the per-tick `/proc/self/{task,fd}`
    /// directory scans. The RSS read itself still shells out to `ps` on macOS.
    pub fn capture_rss() -> Option<usize> {
        rss_bytes()
    }

    /// Growth of `self` (after) over `baseline` (before); see [`ResourceGrowth`].
    pub fn growth_from(&self, baseline: &ResourceSnapshot) -> ResourceGrowth {
        let delta = |after: Option<usize>, before: Option<usize>| {
            before.zip(after).map(|(b, a)| a.saturating_sub(b))
        };
        ResourceGrowth {
            rss: delta(self.rss, baseline.rss),
            threads: delta(self.threads, baseline.threads),
            fds: delta(self.fds, baseline.fds),
        }
    }
}

fn rss_bytes() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            if let Some(val) = line.strip_prefix("VmRSS:") {
                let kb: usize = val.trim().trim_end_matches(" kB").trim().parse().ok()?;
                return Some(kb * 1024);
            }
        }
        None
    }

    #[cfg(target_os = "macos")]
    {
        Some(macos_task_info()?.pti_resident_size as usize)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

fn thread_count() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        Some(std::fs::read_dir("/proc/self/task").ok()?.count())
    }
    #[cfg(target_os = "macos")]
    {
        usize::try_from(macos_task_info()?.pti_threadnum).ok()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// The read's own transient fd closes with the iterator, so before and after
/// samples stay symmetric.
fn fd_count() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        Some(std::fs::read_dir("/proc/self/fd").ok()?.count())
    }
    #[cfg(target_os = "macos")]
    {
        use std::ffi::c_void;

        // A null-buffer probe returns the bytes required for the process's
        // current proc_fdinfo array. This is a point-in-time count; callers
        // compare like-for-like snapshots and tolerate normal short-lived FDs.
        // SAFETY: the documented size probe accepts a null buffer and size 0.
        let bytes = unsafe {
            libc::proc_pidinfo(
                std::process::id() as libc::pid_t,
                libc::PROC_PIDLISTFDS,
                0,
                std::ptr::null_mut::<c_void>(),
                0,
            )
        };
        if bytes < 0 {
            return None;
        }
        usize::try_from(bytes)
            .ok()
            .map(|bytes| bytes / std::mem::size_of::<libc::proc_fdinfo>())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(target_os = "macos")]
fn macos_task_info() -> Option<libc::proc_taskinfo> {
    use std::ffi::c_void;
    use std::mem::{MaybeUninit, size_of};

    let mut info = MaybeUninit::<libc::proc_taskinfo>::zeroed();
    let expected = i32::try_from(size_of::<libc::proc_taskinfo>()).ok()?;
    // SAFETY: `info` is writable for `expected` bytes. A full-size return is
    // required before the initialized value is read.
    let written = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::pid_t,
            libc::PROC_PIDTASKINFO,
            0,
            info.as_mut_ptr().cast::<c_void>(),
            expected,
        )
    };
    (written == expected).then(|| {
        // SAFETY: the full record was initialized by proc_pidinfo above.
        unsafe { info.assume_init() }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_from_saturates_and_propagates_none() {
        let before = ResourceSnapshot {
            rss: Some(100),
            threads: Some(5),
            fds: None,
        };
        let after = ResourceSnapshot {
            rss: Some(30),
            threads: Some(9),
            fds: Some(3),
        };
        let growth = after.growth_from(&before);
        assert_eq!(growth.rss, Some(0), "a shrink saturates to zero");
        assert_eq!(growth.threads, Some(4), "growth is the delta");
        assert_eq!(
            growth.fds, None,
            "a missing baseline sample propagates None"
        );
    }

    #[test]
    fn current_process_snapshot_reports_supported_metrics() {
        let snapshot = ResourceSnapshot::capture();
        assert!(
            snapshot.rss.is_some(),
            "RSS must be observable on release targets"
        );
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            assert!(
                snapshot.threads.is_some_and(|count| count > 0),
                "thread count must be observable"
            );
            assert!(
                snapshot.fds.is_some_and(|count| count > 0),
                "file descriptor count must be observable"
            );
        }
    }
}
