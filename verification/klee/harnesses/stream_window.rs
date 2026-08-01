#![no_main]
#![no_std]

#[path = "../support.rs"]
mod paniccheck;
#[path = "../../../crates/common/xai-grok-verification-core/src/streaming.rs"]
#[allow(dead_code)]
mod production;

use core::ffi::c_char;
use paniccheck::{SymbolicValue as _, require};
use production::stream_window;

#[no_mangle]
pub extern "C" fn main() -> i32 {
    let tail_len = u16::symbolic(b"tail_len\0".as_ptr().cast::<c_char>()) as usize;
    let total = u64::symbolic(b"total\0".as_ptr().cast::<c_char>());
    let last_total = u64::symbolic(b"last_total\0".as_ptr().cast::<c_char>());

    match stream_window(tail_len, total, last_total) {
        None => require(total <= last_total),
        Some(window) => {
            require(total > last_total);
            require(window.start <= tail_len);

            let new_bytes = total - last_total;
            if window.gap {
                require(window.start == 0);
                require(new_bytes > tail_len as u64);
            } else {
                require(new_bytes <= tail_len as u64);
                require(tail_len - window.start == new_bytes as usize);
            }
        }
    }

    0
}
