#![no_main]
#![no_std]

#[path = "../support.rs"]
mod paniccheck;
#[path = "../../../crates/common/xai-grok-verification-core/src/streaming.rs"]
#[allow(dead_code)]
mod production;

use core::ffi::c_char;
use paniccheck::{SymbolicValue as _, require};
use production::advance_stream_cursor;

#[no_mangle]
pub extern "C" fn main() -> i32 {
    let total = u64::symbolic(b"total\0".as_ptr().cast::<c_char>());
    let last_total = u64::symbolic(b"last_total\0".as_ptr().cast::<c_char>());
    let selected_len =
        u16::symbolic(b"selected_len\0".as_ptr().cast::<c_char>()) as usize;
    let emitted_len = u16::symbolic(b"emitted_len\0".as_ptr().cast::<c_char>()) as usize;
    let gap = u8::symbolic(b"gap\0".as_ptr().cast::<c_char>()) & 1 == 1;

    let actual = advance_stream_cursor(total, last_total, selected_len, emitted_len, gap);
    let structurally_valid =
        total > last_total && emitted_len > 0 && emitted_len <= selected_len;

    if !structurally_valid {
        require(actual.is_none());
        return 0;
    }

    let expected = if gap {
        let deferred = (selected_len - emitted_len) as u64;
        total
            .checked_sub(deferred)
            .filter(|next| *next > last_total)
    } else {
        last_total
            .checked_add(emitted_len as u64)
            .filter(|next| *next <= total)
    };

    require(actual == expected);
    if let Some(next) = actual {
        require(next > last_total);
        require(next <= total);
    }

    0
}
