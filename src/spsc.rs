//! Single-producer single-consumer lock-free ring buffer.
//!
//! Uses `Cell` for interior mutability in single-threaded contexts.
//! The producer and consumer share the head and tail indices.

use std::cell::Cell;
use std::rc::Rc;

struct Shared<T> {
    data: Vec<Cell<Option<T>>>,
    head: Cell<usize>,
    tail: Cell<usize>,
    capacity: usize,
}

/// SPSC ring buffer producer handle.
pub struct Producer<T> {
    shared: Rc<Shared<T>>,
}

/// SPSC ring buffer consumer handle.
pub struct Consumer<T> {
    shared: Rc<Shared<T>>,
}

/// Create a new SPSC ring buffer, returning (producer, consumer).
pub fn spsc_new<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let shared = Rc::new(Shared {
        data: (0..capacity).map(|_| Cell::new(None)).collect(),
        head: Cell::new(0),
        tail: Cell::new(0),
        capacity,
    });
    let producer = Producer { shared: shared.clone() };
    let consumer = Consumer { shared };
    (producer, consumer)
}

impl<T> Producer<T> {
    /// Push an item. Returns `Err(item)` if full.
    pub fn push(&self, item: T) -> Result<(), T> {
        let tail = self.shared.tail.get();
        let head = self.shared.head.get();
        let next_tail = (tail + 1) % self.shared.capacity;
        if next_tail == head {
            return Err(item);
        }
        self.shared.data[tail].set(Some(item));
        self.shared.tail.set(next_tail);
        Ok(())
    }

    /// Returns true if full.
    pub fn is_full(&self) -> bool {
        let tail = self.shared.tail.get();
        let head = self.shared.head.get();
        (tail + 1) % self.shared.capacity == head
    }
}

impl<T> Consumer<T> {
    /// Pop an item.
    pub fn pop(&self) -> Option<T> {
        let head = self.shared.head.get();
        let tail = self.shared.tail.get();
        if head == tail {
            return None;
        }
        let item = self.shared.data[head].take();
        self.shared.head.set((head + 1) % self.shared.capacity);
        item
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.shared.head.get() == self.shared.tail.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_push_pop() {
        let (prod, cons) = spsc_new(4);
        prod.push(1).unwrap();
        prod.push(2).unwrap();
        assert_eq!(cons.pop(), Some(1));
        assert_eq!(cons.pop(), Some(2));
        assert_eq!(cons.pop(), None);
    }

    #[test]
    fn full_returns_err() {
        let (prod, _) = spsc_new::<i32>(3);
        prod.push(1).unwrap();
        prod.push(2).unwrap();
        assert!(prod.push(3).is_err());
    }

    #[test]
    fn empty_returns_none() {
        let (_, cons) = spsc_new::<i32>(4);
        assert!(cons.is_empty());
        assert_eq!(cons.pop(), None);
    }

    #[test]
    fn interleaved_push_pop() {
        let (prod, cons) = spsc_new(4);
        for i in 0..50 {
            prod.push(i).unwrap();
            assert_eq!(cons.pop(), Some(i));
        }
    }

    #[test]
    fn wrap_around() {
        let (prod, cons) = spsc_new(3);
        prod.push(1).unwrap();
        prod.push(2).unwrap();
        assert_eq!(cons.pop(), Some(1));
        prod.push(3).unwrap();
        assert_eq!(cons.pop(), Some(2));
        assert_eq!(cons.pop(), Some(3));
    }

    #[test]
    fn is_full_and_empty() {
        let (prod, cons) = spsc_new(3);
        assert!(cons.is_empty());
        assert!(!prod.is_full());
        prod.push(1).unwrap();
        prod.push(2).unwrap();
        assert!(prod.is_full());
        assert!(!cons.is_empty());
    }
}
