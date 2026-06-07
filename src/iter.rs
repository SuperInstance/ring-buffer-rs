//! Ring buffer iterator support.

use crate::buffer::RingBuffer;

/// Iterator over elements of a ring buffer (by reference).
pub struct RingIter<'a, T> {
    buf: &'a RingBuffer<T>,
    pos: usize,
    consumed: usize,
}

impl<'a, T> RingIter<'a, T> {
    /// Create a new iterator starting from the buffer's head.
    pub fn new(buf: &'a RingBuffer<T>) -> Self {
        Self {
            buf,
            pos: buf.head(),
            consumed: 0,
        }
    }
}

impl<'a, T> Iterator for RingIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.consumed >= self.buf.len() {
            return None;
        }
        let item = self.buf.get_at(self.pos);
        self.pos = (self.pos + 1) % self.buf.capacity();
        self.consumed += 1;
        item
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.buf.len() - self.consumed;
        (rem, Some(rem))
    }
}

impl<'a, T> ExactSizeIterator for RingIter<'a, T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterate_in_order() {
        let mut buf = RingBuffer::new(4);
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();
        let items: Vec<&i32> = RingIter::new(&buf).collect();
        assert_eq!(items, vec![&1, &2, &3]);
    }

    #[test]
    fn iterate_after_wrap() {
        let mut buf = RingBuffer::new(3);
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();
        buf.pop();
        buf.push(4).unwrap();
        let items: Vec<&i32> = RingIter::new(&buf).collect();
        assert_eq!(items, vec![&2, &3, &4]);
    }

    #[test]
    fn iterate_empty() {
        let buf: RingBuffer<i32> = RingBuffer::new(4);
        let items: Vec<&i32> = RingIter::new(&buf).collect();
        assert!(items.is_empty());
    }

    #[test]
    fn size_hint_matches() {
        let mut buf = RingBuffer::new(4);
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        let mut iter = RingIter::new(&buf);
        assert_eq!(iter.size_hint(), (2, Some(2)));
        iter.next();
        assert_eq!(iter.size_hint(), (1, Some(1)));
    }
}
