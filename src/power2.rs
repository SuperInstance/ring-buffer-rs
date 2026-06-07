//! Power-of-two sized ring buffer.
//!
//! Uses bitwise AND for fast modulo: `index % capacity` → `index & (capacity - 1)`.
//! Capacity must be a power of two.

/// Power-of-two sized ring buffer with fast modulo.
pub struct Power2Buffer<T> {
    data: Vec<Option<T>>,
    mask: usize,
    head: usize,
    tail: usize,
    len: usize,
}

impl<T> Power2Buffer<T> {
    /// Create a new buffer. `capacity` must be a power of two.
    ///
    /// # Panics
    ///
    /// Panics if capacity is 0 or not a power of two.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0 && capacity.is_power_of_two(), "capacity must be a power of two");
        Self {
            data: (0..capacity).map(|_| None).collect(),
            mask: capacity - 1,
            head: 0,
            tail: 0,
            len: 0,
        }
    }

    /// Push an item. Returns `Err(item)` if full.
    pub fn push(&mut self, item: T) -> Result<(), T> {
        if self.len == self.data.len() {
            return Err(item);
        }
        self.data[self.tail] = Some(item);
        self.tail = (self.tail + 1) & self.mask;
        self.len += 1;
        Ok(())
    }

    /// Pop an item.
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let item = self.data[self.head].take();
        self.head = (self.head + 1) & self.mask;
        self.len -= 1;
        item
    }

    /// Number of items.
    pub fn len(&self) -> usize { self.len }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// Capacity.
    pub fn capacity(&self) -> usize { self.data.len() }

    /// Returns the mask used for fast modulo.
    pub fn mask(&self) -> usize { self.mask }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_pop() {
        let mut buf = Power2Buffer::new(4);
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        assert_eq!(buf.pop(), Some(1));
        assert_eq!(buf.pop(), Some(2));
    }

    #[test]
    fn wrap_around_fast() {
        let mut buf = Power2Buffer::new(4);
        for i in 0..8 {
            buf.push(i).unwrap();
            assert_eq!(buf.pop(), Some(i));
        }
    }

    #[test]
    fn full_returns_err() {
        let mut buf = Power2Buffer::new(4);
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();
        buf.push(4).unwrap();
        assert!(buf.push(5).is_err());
    }

    #[test]
    fn capacity_is_power_of_two() {
        let buf: Power2Buffer<i32> = Power2Buffer::new(16);
        assert_eq!(buf.capacity(), 16);
        assert_eq!(buf.mask(), 15);
    }

    #[test]
    #[should_panic(expected = "capacity must be a power of two")]
    fn rejects_non_power_of_two() {
        let _: Power2Buffer<i32> = Power2Buffer::new(3);
    }

    #[test]
    #[should_panic(expected = "capacity must be a power of two")]
    fn rejects_zero() {
        let _: Power2Buffer<i32> = Power2Buffer::new(0);
    }

    #[test]
    fn len_and_empty() {
        let mut buf = Power2Buffer::new(8);
        assert!(buf.is_empty());
        buf.push(1).unwrap();
        assert!(!buf.is_empty());
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn large_buffer() {
        let mut buf = Power2Buffer::new(1024);
        for i in 0..1024 {
            buf.push(i).unwrap();
        }
        assert_eq!(buf.len(), 1024);
        for i in 0..1024 {
            assert_eq!(buf.pop(), Some(i));
        }
        assert!(buf.is_empty());
    }
}
