//! # ring-buffer-rs
//!
//! Circular/ring buffer implementations in pure Rust with zero external dependencies.
//!
//! ## Buffers
//!
//! - **Buffer** — Standard circular buffer (ring buffer).
//! - **Spsc** — Single-producer single-consumer lock-free ring buffer.
//! - **Power2** — Power-of-two sized ring buffer with fast modulo.
//! - **Iter** — Ring buffer iterator support.
//! - **Window** — Sliding window over a ring buffer.
//!
//! ## Example
//!
//! ```
//! use ring_buffer_rs::buffer::RingBuffer;
//!
//! let mut buf = RingBuffer::new(4);
//! buf.push(1);
//! buf.push(2);
//! buf.push(3);
//! assert_eq!(buf.pop(), Some(1));
//! assert_eq!(buf.pop(), Some(2));
//! ```

pub mod buffer;
pub mod spsc;
pub mod power2;
pub mod iter;
pub mod window;

pub use buffer::RingBuffer;
