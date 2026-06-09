//! # ring-buffer-rs Tutorial
//!
//! A progressive guide to ring buffers in Rust.
//! Run with: `cargo run --example tutorial`
//!
//! ## Lessons
//! 1. The basics — push and pop
//! 2. Capacity management — full buffers and remaining space
//! 3. Overwrite mode — sliding window behavior
//! 4. Wrap-around — the circular nature
//! 5. Iteration — reading all elements
//! 6. SPSC — lock-free single-producer single-consumer
//! 7. Power2Buffer — fast modulo with power-of-two sizes
//! 8. SlidingWindow — streaming analytics

use ring_buffer_rs::buffer::RingBuffer;
use ring_buffer_rs::iter::RingIter;
use ring_buffer_rs::power2::Power2Buffer;
use ring_buffer_rs::spsc;
use ring_buffer_rs::window::SlidingWindow;

fn separator(title: &str) {
    println!();
    println!("═══════════════════════════════════════════════════════════");
    println!("  Lesson: {}", title);
    println!("═══════════════════════════════════════════════════════════");
    println!();
}

fn main() {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║         ring-buffer-rs Tutorial                          ║");
    println!("║   Circular Buffers in Pure Rust                          ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    lesson_1_basics();
    lesson_2_capacity();
    lesson_3_overwrite();
    lesson_4_wrap_around();
    lesson_5_iteration();
    lesson_6_spsc();
    lesson_7_power2();
    lesson_8_sliding_window();

    println!();
    println!("✅ Tutorial complete! You've mastered ring buffers in Rust.");
}

// ── Lesson 1 ────────────────────────────────────────────────────────
fn lesson_1_basics() {
    separator("1. The Basics — Push and Pop");

    println!("A ring buffer is a fixed-capacity FIFO queue that wraps around.\n");

    let mut buf: RingBuffer<i32> = RingBuffer::new(4);
    println!("📦 Created buffer with capacity 4");
    println!("   len: {}, empty: {}, full: {}", buf.len(), buf.is_empty(), buf.is_full());

    // Push items
    buf.push(10).unwrap();
    buf.push(20).unwrap();
    buf.push(30).unwrap();
    println!("\n➡️  Pushed 10, 20, 30");
    println!("   len: {}, empty: {}, full: {}", buf.len(), buf.is_empty(), buf.is_full());

    // Peek at front
    println!("\n👀 Peek at front: {:?}", buf.peek());

    // Pop items — FIFO order!
    println!("\n⬅️  Popping all items (FIFO order):");
    while let Some(item) = buf.pop() {
        println!("   Popped: {}", item);
    }
    println!("   Buffer is now empty: {}", buf.is_empty());
}

// ── Lesson 2 ────────────────────────────────────────────────────────
fn lesson_2_capacity() {
    separator("2. Capacity Management — Full Buffers");

    println!("When the buffer is full, push returns Err(item).\n");

    let mut buf: RingBuffer<&str> = RingBuffer::new(3);

    buf.push("first").unwrap();
    buf.push("second").unwrap();
    buf.push("third").unwrap();

    println!("📦 Pushed 3 items into capacity-3 buffer:");
    println!("   len: {}, capacity: {}, remaining: {}", buf.len(), buf.capacity(), buf.remaining());
    println!("   is_full: {}", buf.is_full());

    // Try to push when full
    let result = buf.push("overflow");
    println!("\n❌ Tried to push 'overflow': {:?}", result);
    println!("   The item is returned in Err — nothing is lost!");

    // Pop one to make room
    buf.pop();
    println!("\n⬅️  Popped one item, now remaining: {}", buf.remaining());
    buf.push("new item").unwrap();
    println!("➡️  Pushed 'new item' successfully!");

    // Clear
    buf.clear();
    println!("\n🧹 Cleared buffer: len = {}, is_empty = {}", buf.len(), buf.is_empty());
}

// ── Lesson 3 ────────────────────────────────────────────────────────
fn lesson_3_overwrite() {
    separator("3. Overwrite Mode — Sliding Window Behavior");

    println!("push_overwrite() always succeeds, evicting the oldest item\n");
    println!("when the buffer is full. Great for keeping recent data.\n");

    let mut buf: RingBuffer<&str> = RingBuffer::new(3);

    buf.push_overwrite("A");
    buf.push_overwrite("B");
    buf.push_overwrite("C");
    println!("📦 Pushed A, B, C (buffer full):");
    println!("   Contents: A, B, C");

    // Overwrite evicts A
    let evicted = buf.push_overwrite("D");
    println!("\n➡️  push_overwrite('D'):");
    println!("   Evicted: {:?}", evicted);
    println!("   Contents: B, C, D");

    // More overwrites
    let evicted = buf.push_overwrite("E");
    println!("\n➡️  push_overwrite('E'):");
    println!("   Evicted: {:?}", evicted);

    let evicted = buf.push_overwrite("F");
    println!("\n➡️  push_overwrite('F'):");
    println!("   Evicted: {:?}", evicted);

    println!("\n📋 Final contents:");
    while let Some(item) = buf.pop() {
        println!("   {}", item);
    }
}

// ── Lesson 4 ────────────────────────────────────────────────────────
fn lesson_4_wrap_around() {
    separator("4. Wrap-Around — The Circular Nature");

    println!("The buffer wraps indices around internally. Let's see it in action.\n");

    let mut buf: RingBuffer<i32> = RingBuffer::new(3);

    // Fill completely
    buf.push(1).unwrap();
    buf.push(2).unwrap();
    buf.push(3).unwrap();
    println!("📦 Filled buffer: [1, 2, 3]");

    // Pop two
    buf.pop();
    buf.pop();
    println!("⬅️  Popped 2 items (removed 1, 2)");

    // Push two more — these wrap around!
    buf.push(4).unwrap();
    buf.push(5).unwrap();
    println!("➡️  Pushed 4, 5 (wraps around internally)");

    println!("\n📋 Reading all items (should be 3, 4, 5):");
    while let Some(item) = buf.pop() {
        println!("   {}", item);
    }

    // Demonstrate many cycles
    println!("\n🔄 100 push/pop cycles on capacity-5 buffer:");
    let mut buf2: RingBuffer<i32> = RingBuffer::new(5);
    for i in 0..100 {
        if buf2.is_full() {
            buf2.pop();
        }
        buf2.push(i).unwrap();
    }
    println!("   Final contents:");
    while let Some(v) = buf2.pop() {
        print!(" {} ", v);
    }
    println!("\n   (Last 5 values: 95-99)");
}

// ── Lesson 5 ────────────────────────────────────────────────────────
fn lesson_5_iteration() {
    separator("5. Iteration — Reading All Elements");

    println!("RingIter provides ordered iteration over buffer contents.\n");

    let mut buf: RingBuffer<&str> = RingBuffer::new(5);
    buf.push("hello").unwrap();
    buf.push("world").unwrap();
    buf.push("rust").unwrap();

    println!("📦 Buffer contents via iterator:");
    for (i, item) in RingIter::new(&buf).enumerate() {
        println!("   [{}] {}", i, item);
    }

    // Iteration after wrap-around
    buf.pop(); // remove "hello"
    buf.push("buffers").unwrap();
    buf.push("rule").unwrap();

    println!("\n📦 After wrap-around:");
    for item in RingIter::new(&buf) {
        println!("   {}", item);
    }

    // ExactSizeIterator
    let mut iter = RingIter::new(&buf);
    println!("\n📏 ExactSizeIterator support:");
    println!("   Remaining: {}", iter.len());
    iter.next();
    println!("   After 1 next: {}", iter.len());

    // Empty buffer iteration
    let empty: RingBuffer<i32> = RingBuffer::new(4);
    let count = RingIter::new(&empty).count();
    println!("\n   Empty buffer iterator count: {}", count);
}

// ── Lesson 6 ────────────────────────────────────────────────────────
fn lesson_6_spsc() {
    separator("6. SPSC — Lock-Free Single-Producer Single-Consumer");

    println!("The SPSC buffer splits into Producer and Consumer handles.\n");
    println!("Producer pushes, Consumer pops — no locks needed.\n");

    let (producer, consumer) = spsc::spsc_new::<i32>(5);

    // Producer sends data
    println!("🏭 Producer sending: 10, 20, 30");
    producer.push(10).unwrap();
    producer.push(20).unwrap();
    producer.push(30).unwrap();
    println!("   Producer is_full: {}", producer.is_full());

    // Consumer receives
    println!("\n🛒 Consumer receiving:");
    while let Some(item) = consumer.pop() {
        println!("   Received: {}", item);
    }
    println!("   Consumer is_empty: {}", consumer.is_empty());

    // Interleaved operation
    println!("\n🔄 Interleaved push/pop (streaming pattern):");
    for i in 0..5 {
        producer.push(i * 100).unwrap();
        let val = consumer.pop().unwrap();
        println!("   Sent: {}, Received: {}", i * 100, val);
    }

    // Full buffer behavior
    let (prod2, _) = spsc::spsc_new::<i32>(3);
    prod2.push(1).unwrap();
    prod2.push(2).unwrap();
    println!("\n📦 Capacity-3 SPSC after 2 pushes:");
    println!("   is_full: {} (needs one more to fill)", prod2.is_full());
    prod2.push(3).unwrap();
    println!("   After 3rd push, is_full: {}", prod2.is_full());
    let result = prod2.push(4);
    println!("   4th push: {:?}", result);
}

// ── Lesson 7 ────────────────────────────────────────────────────────
fn lesson_7_power2() {
    separator("7. Power2Buffer — Fast Modulo with Power-of-Two Sizes");

    println!("Power2Buffer uses bitwise AND instead of modulo for index");
    println!("wrapping. Capacity MUST be a power of two.\n");

    // Create power-of-two buffer
    let mut buf: Power2Buffer<i32> = Power2Buffer::new(8);
    println!("📦 Created Power2Buffer(capacity=8)");
    println!("   mask: {} (= 0b{:07b}, used for index & mask)", buf.mask(), buf.mask());

    // Fast push/pop
    for i in 0..8 {
        buf.push(i).unwrap();
    }
    println!("\n➡️  Pushed 0..8:");
    println!("   len: {}, capacity: {}, is_empty: {}", buf.len(), buf.capacity(), buf.is_empty());

    println!("\n⬅️  Popping all:");
    while let Some(v) = buf.pop() {
        print!(" {} ", v);
    }
    println!();

    // Wrap-around stress test
    println!("\n🔄 1000 push/pop cycles on capacity-4 buffer:");
    let mut buf2: Power2Buffer<i32> = Power2Buffer::new(4);
    for i in 0..1000 {
        buf2.push(i).unwrap();
        assert_eq!(buf2.pop(), Some(i));
    }
    println!("   All 1000 cycles completed ✓");
    println!("   is_empty: {}", buf2.is_empty());

    // Large buffer
    let mut large: Power2Buffer<u64> = Power2Buffer::new(1024);
    for i in 0..1024 {
        large.push(i).unwrap();
    }
    println!("\n📦 Large buffer (capacity=1024):");
    println!("   Full: {}, len: {}", !large.is_empty(), large.len());
    let sum: u64 = (0..1024).map(|_| large.pop().unwrap()).sum();
    println!("   Sum of 0..1024 = {} (expected 523776)", sum);
}

// ── Lesson 8 ────────────────────────────────────────────────────────
fn lesson_8_sliding_window() {
    separator("8. SlidingWindow — Streaming Analytics");

    println!("SlidingWindow keeps the N most recent values and computes");
    println!("running statistics. Perfect for streaming data.\n");

    // Basic sliding window
    let mut win: SlidingWindow<i32> = SlidingWindow::new(3);
    println!("📦 Created SlidingWindow(capacity=3)");

    win.push(10);
    win.push(20);
    win.push(30);
    println!("\n➡️  Pushed 10, 20, 30");
    println!("   len: {}, oldest: {:?}", win.len(), win.oldest());
    println!("   total_pushed: {}", win.total_pushed());

    win.push(40);
    println!("\n➡️  Pushed 40 (evicts 10)");
    println!("   len: {}, oldest: {:?}", win.len(), win.oldest());
    println!("   total_pushed: {} (tracks lifetime pushes)", win.total_pushed());

    // Moving average with f64 window
    let mut avg: SlidingWindow<f64> = SlidingWindow::new(5);
    println!("\n📊 5-point moving average of sensor data:");
    println!("   {:>8} {:>12} {:>12}", "Value", "Window", "Average");
    println!("   {}", "-".repeat(36));

    let readings = [10.0, 12.0, 15.0, 11.0, 13.0, 20.0, 18.0, 25.0];
    for val in readings {
        avg.push(val);
        let mean = avg.mean().unwrap();
        println!("   {:>8.1} {:>5}/{:<5} {:>12.2}", val, avg.len(), avg.capacity(), mean);
    }

    // Demonstrate convergence
    println!("\n📈 Sliding average converges with constant input:");
    let mut conv: SlidingWindow<f64> = SlidingWindow::new(10);
    for i in 0..15 {
        conv.push(42.0);
        if i >= 8 {
            println!("   After {} pushes: mean = {:.4}", i + 1, conv.mean().unwrap());
        }
    }
    println!("   → Once full, mean is exact: {:.1}", conv.mean().unwrap());
    println!("   capacity: {}, total_pushed: {}", conv.capacity(), conv.total_pushed());
}
