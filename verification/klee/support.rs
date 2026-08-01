#![allow(dead_code)]

use core::ffi::{c_char, c_void};
use core::mem::MaybeUninit;
use core::panic::PanicInfo;

extern "C" {
    fn klee_abort() -> !;
    fn klee_assume(condition: usize);
    fn klee_make_symbolic(address: *mut c_void, bytes: usize, name: *const c_char);
}

/// Strategy interface for constructing KLEE-backed symbolic Rust values.
///
/// This is the small, compiler-compatible counterpart of PanicCheck's
/// per-type symbolic variable strategies. Implementations are deliberately
/// restricted to plain integers and fixed byte arrays; complex collections,
/// strings, pointers, and concurrent code are outside this bounded harness.
pub trait SymbolicValue: Sized {
    fn symbolic(name: *const c_char) -> Self;
}

macro_rules! integer_symbolic_value {
    ($($type:ty),+ $(,)?) => {
        $(
            impl SymbolicValue for $type {
                fn symbolic(name: *const c_char) -> Self {
                    let mut value = MaybeUninit::<Self>::uninit();
                    unsafe {
                        klee_make_symbolic(
                            value.as_mut_ptr().cast(),
                            core::mem::size_of::<Self>(),
                            name,
                        );
                        value.assume_init()
                    }
                }
            }
        )+
    };
}

integer_symbolic_value!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl<const N: usize> SymbolicValue for [u8; N] {
    fn symbolic(name: *const c_char) -> Self {
        let mut value = MaybeUninit::<Self>::uninit();
        unsafe {
            klee_make_symbolic(
                value.as_mut_ptr().cast(),
                core::mem::size_of::<Self>(),
                name,
            );
            value.assume_init()
        }
    }
}

pub fn assume(condition: bool) {
    unsafe { klee_assume(condition as usize) }
}

pub fn require(condition: bool) {
    if !condition {
        unsafe { klee_abort() }
    }
}

#[panic_handler]
fn panic(_info: &PanicInfo<'_>) -> ! {
    unsafe { klee_abort() }
}
