//! Sliding window over a ring buffer.
//!
//! Maintains a fixed-size window of the most recent values pushed.

use crate::buffer::RingBuffer;

/// A sliding window of fixed size over a stream of values.
pub struct SlidingWindow<T> {
    buf: RingBuffer<T>,
    total_pushed: usize,
}

impl<T> SlidingWindow<T> {
    /// Create a new sliding window with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: RingBuffer::new(capacity),
            total_pushed: 0,
        }
    }

    /// Push a value into the window. If the window is full, the oldest value is evicted.
    pub fn push(&mut self, value: T) {
        self.buf.push_overwrite(value);
        self.total_pushed += 1;
    }

    /// Current number of values in the window.
    pub fn len(&self) -> usize { self.buf.len() }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool { self.buf.is_empty() }

    /// Window capacity.
    pub fn capacity(&self) -> usize { self.buf.capacity() }

    /// Total number of values ever pushed.
    pub fn total_pushed(&self) -> usize { self.total_pushed }

    /// Get the oldest value in the window.
    pub fn oldest(&self) -> Option<&T> { self.buf.peek() }
}

impl SlidingWindow<f64> {
    /// Compute the mean of all values in the window.
    pub fn mean(&self) -> Option<f64> {
        if self.is_empty() {
            return None;
        }
        use crate::iter::RingIter;
        let sum: f64 = RingIter::new(&self.buf).sum();
        Some(sum / self.len() as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_window() {
        let mut win = SlidingWindow::new(3);
        win.push(1);
        win.push(2);
        win.push(3);
        assert_eq!(win.len(), 3);
        assert_eq!(win.oldest(), Some(&1));
    }

    #[test]
    fn evicts_oldest() {
        let mut win = SlidingWindow::new(3);
        win.push(1);
        win.push(2);
        win.push(3);
        win.push(4);
        assert_eq!(win.len(), 3);
        assert_eq!(win.oldest(), Some(&2));
    }

    #[test]
    fn total_pushed() {
        let mut win = SlidingWindow::new(2);
        win.push(1);
        win.push(2);
        win.push(3);
        win.push(4);
        assert_eq!(win.total_pushed(), 4);
        assert_eq!(win.len(), 2);
    }

    #[test]
    fn mean_calculation() {
        let mut win = SlidingWindow::new(3);
        win.push(1.0);
        win.push(2.0);
        win.push(3.0);
        assert!((win.mean().unwrap() - 2.0).abs() < 1e-10);
        win.push(6.0);
        // window: [2.0, 3.0, 6.0]
        assert!((win.mean().unwrap() - (2.0 + 3.0 + 6.0) / 3.0).abs() < 1e-10);
    }

    #[test]
    fn empty_mean() {
        let win: SlidingWindow<f64> = SlidingWindow::new(5);
        assert!(win.mean().is_none());
    }

    #[test]
    fn capacity() {
        let win: SlidingWindow<i32> = SlidingWindow::new(10);
        assert_eq!(win.capacity(), 10);
    }

    #[test]
    fn is_empty() {
        let mut win: SlidingWindow<i32> = SlidingWindow::new(5);
        assert!(win.is_empty());
        win.push(1);
        assert!(!win.is_empty());
    }

    #[test]
    fn sliding_average_converges() {
        let mut win = SlidingWindow::new(10);
        for _ in 0..100 {
            win.push(5.0);
        }
        assert!((win.mean().unwrap() - 5.0).abs() < 1e-10);
    }
}
