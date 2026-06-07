//! Standard circular buffer (ring buffer).

/// A circular buffer of fixed capacity.
pub struct RingBuffer<T> {
    data: Vec<Option<T>>,
    head: usize,
    tail: usize,
    len: usize,
}

impl<T> RingBuffer<T> {
    /// Create a new ring buffer with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            data: (0..capacity).map(|_| None).collect(),
            head: 0,
            tail: 0,
            len: 0,
        }
    }

    /// Push an item to the back. Returns `Err(item)` if full.
    pub fn push(&mut self, item: T) -> Result<(), T> {
        if self.len == self.data.len() {
            return Err(item);
        }
        self.data[self.tail] = Some(item);
        self.tail = (self.tail + 1) % self.data.len();
        self.len += 1;
        Ok(())
    }

    /// Push an item, overwriting the oldest if full. Returns the evicted item if any.
    pub fn push_overwrite(&mut self, item: T) -> Option<T> {
        if self.len == self.data.len() {
            let old = self.data[self.head].take();
            self.data[self.tail] = Some(item);
            self.head = (self.head + 1) % self.data.len();
            self.tail = (self.tail + 1) % self.data.len();
            old
        } else {
            self.data[self.tail] = Some(item);
            self.tail = (self.tail + 1) % self.data.len();
            self.len += 1;
            None
        }
    }

    /// Pop an item from the front.
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let item = self.data[self.head].take();
        self.head = (self.head + 1) % self.data.len();
        self.len -= 1;
        item
    }

    /// Peek at the front item without removing it.
    pub fn peek(&self) -> Option<&T> {
        if self.len == 0 {
            None
        } else {
            self.data[self.head].as_ref()
        }
    }

    /// Number of items currently in the buffer.
    pub fn len(&self) -> usize { self.len }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// Returns true if full.
    pub fn is_full(&self) -> bool { self.len == self.data.len() }

    /// Total capacity.
    pub fn capacity(&self) -> usize { self.data.len() }

    /// Remaining space.
    pub fn remaining(&self) -> usize { self.data.len() - self.len }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        for slot in &mut self.data {
            *slot = None;
        }
        self.head = 0;
        self.tail = 0;
        self.len = 0;
    }

    /// Get the head index (for iterators).
    pub(crate) fn head(&self) -> usize { self.head }

    /// Get a reference to the item at the given internal index.
    pub(crate) fn get_at(&self, index: usize) -> Option<&T> {
        self.data[index].as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_pop() {
        let mut buf = RingBuffer::new(4);
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        assert_eq!(buf.pop(), Some(1));
        assert_eq!(buf.pop(), Some(2));
        assert_eq!(buf.pop(), None);
    }

    #[test]
    fn full_returns_err() {
        let mut buf = RingBuffer::new(2);
        assert!(buf.push(1).is_ok());
        assert!(buf.push(2).is_ok());
        assert!(buf.push(3).is_err());
    }

    #[test]
    fn overwrite_evicts_oldest() {
        let mut buf = RingBuffer::new(2);
        buf.push_overwrite(1);
        buf.push_overwrite(2);
        let evicted = buf.push_overwrite(3);
        assert_eq!(evicted, Some(1));
        assert_eq!(buf.pop(), Some(2));
        assert_eq!(buf.pop(), Some(3));
    }

    #[test]
    fn wrap_around() {
        let mut buf = RingBuffer::new(3);
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();
        assert_eq!(buf.pop(), Some(1));
        buf.push(4).unwrap();
        assert_eq!(buf.pop(), Some(2));
        assert_eq!(buf.pop(), Some(3));
        assert_eq!(buf.pop(), Some(4));
    }

    #[test]
    fn len_and_empty() {
        let mut buf = RingBuffer::new(4);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        buf.push(1).unwrap();
        assert!(!buf.is_empty());
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn full_and_remaining() {
        let mut buf = RingBuffer::new(3);
        assert!(!buf.is_full());
        assert_eq!(buf.remaining(), 3);
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();
        assert!(buf.is_full());
        assert_eq!(buf.remaining(), 0);
    }

    #[test]
    fn capacity() {
        let buf: RingBuffer<i32> = RingBuffer::new(10);
        assert_eq!(buf.capacity(), 10);
    }

    #[test]
    fn peek() {
        let mut buf = RingBuffer::new(4);
        assert_eq!(buf.peek(), None);
        buf.push(42).unwrap();
        assert_eq!(buf.peek(), Some(&42));
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn clear() {
        let mut buf = RingBuffer::new(4);
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.pop(), None);
    }

    #[test]
    fn many_push_pop() {
        let mut buf = RingBuffer::new(5);
        for i in 0..100 {
            if buf.is_full() {
                buf.pop();
            }
            buf.push(i).unwrap();
        }
        assert_eq!(buf.len(), 5);
    }
}
