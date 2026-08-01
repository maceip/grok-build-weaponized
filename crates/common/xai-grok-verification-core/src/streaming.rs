use core::convert::TryFrom as _;

/// The surviving byte range for one monotonic streaming tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamWindow {
    /// Start offset in the caller's tail buffer. The end is always the tail
    /// length supplied to [`stream_window`].
    pub start: usize,
    /// The monotonic source advanced farther than the surviving tail; bytes
    /// between the previous cursor and the tail were irretrievably dropped.
    pub gap: bool,
}

/// Select the surviving suffix for a monotonic byte source.
///
/// This is intentionally isolated from allocation and serialization. It is a
/// panic-sensitive boundary: a wrong offset is later used to slice a live
/// output buffer. Keep the `paniccheck-target` marker paired with a KLEE
/// harness under `verification/klee/harnesses`.
#[doc = "paniccheck-target: stream_window"]
pub fn stream_window(tail_len: usize, total: u64, last_total: u64) -> Option<StreamWindow> {
    if total <= last_total {
        return None;
    }

    let new_bytes = total - last_total;
    let Ok(tail_len_u64) = u64::try_from(tail_len) else {
        // A theoretical target with usize wider than u64 cannot describe its
        // complete tail using the protocol's u64 monotonic counter. Treat the
        // tail as containing all representable new bytes.
        let new_len = usize::try_from(new_bytes).ok()?;
        return Some(StreamWindow {
            start: tail_len.checked_sub(new_len)?,
            gap: false,
        });
    };

    if new_bytes <= tail_len_u64 {
        let new_len = usize::try_from(new_bytes).ok()?;
        Some(StreamWindow {
            start: tail_len.checked_sub(new_len)?,
            gap: false,
        })
    } else {
        Some(StreamWindow {
            start: 0,
            gap: true,
        })
    }
}

/// Advance a monotonic stream cursor after emitting a prefix of the selected
/// window.
///
/// Invalid caller relationships return `None` rather than wrapping or
/// panicking. In the gap case, bytes already lost upstream count as consumed;
/// otherwise only emitted bytes advance the cursor.
#[doc = "paniccheck-target: advance_stream_cursor"]
pub fn advance_stream_cursor(
    total: u64,
    last_total: u64,
    selected_len: usize,
    emitted_len: usize,
    gap: bool,
) -> Option<u64> {
    if total <= last_total || emitted_len == 0 || emitted_len > selected_len {
        return None;
    }

    if gap {
        let deferred = selected_len.checked_sub(emitted_len)?;
        let deferred = u64::try_from(deferred).ok()?;
        let next = total.checked_sub(deferred)?;
        (next > last_total && next <= total).then_some(next)
    } else {
        let emitted = u64::try_from(emitted_len).ok()?;
        let next = last_total.checked_add(emitted)?;
        (next <= total).then_some(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_uses_new_suffix_when_it_survives() {
        assert_eq!(
            stream_window(5, 5, 2),
            Some(StreamWindow {
                start: 2,
                gap: false,
            })
        );
    }

    #[test]
    fn window_marks_a_gap_when_new_bytes_exceed_tail() {
        assert_eq!(
            stream_window(4, 100, 0),
            Some(StreamWindow {
                start: 0,
                gap: true,
            })
        );
    }

    #[test]
    fn window_rejects_non_monotonic_updates() {
        assert_eq!(stream_window(4, 10, 10), None);
        assert_eq!(stream_window(4, 9, 10), None);
    }

    #[test]
    fn normal_cursor_advances_by_emitted_bytes() {
        assert_eq!(advance_stream_cursor(9, 0, 9, 4, false), Some(4));
        assert_eq!(advance_stream_cursor(9, 4, 5, 4, false), Some(8));
    }

    #[test]
    fn gap_cursor_accounts_for_lost_middle() {
        assert_eq!(advance_stream_cursor(100, 0, 4, 4, true), Some(100));
        assert_eq!(advance_stream_cursor(100, 0, 4, 2, true), Some(98));
    }

    #[test]
    fn invalid_cursor_relationships_are_rejected() {
        assert_eq!(advance_stream_cursor(0, 0, 1, 1, false), None);
        assert_eq!(advance_stream_cursor(4, 0, 4, 0, false), None);
        assert_eq!(advance_stream_cursor(4, 0, 2, 3, false), None);
        assert_eq!(advance_stream_cursor(4, 3, 4, 2, false), None);
    }

    #[test]
    fn arithmetic_boundaries_do_not_wrap() {
        assert_eq!(
            stream_window(1, u64::MAX, u64::MAX - 1),
            Some(StreamWindow {
                start: 0,
                gap: false
            })
        );
        assert_eq!(
            advance_stream_cursor(u64::MAX, u64::MAX - 1, 1, 1, false),
            Some(u64::MAX)
        );
        assert_eq!(
            advance_stream_cursor(u64::MAX, u64::MAX - 1, usize::MAX, 1, true),
            None
        );
    }
}
