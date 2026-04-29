#include <gtest/gtest.h>
#include "bounded/mpmc.hpp"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

template <typename Pred>
static bool WaitFor(Pred pred, std::chrono::milliseconds timeout = 1'000ms) {
    auto deadline = Clock::now() + timeout;
    while (!pred()) {
        if (Clock::now() > deadline) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

static auto MakeProducer(int value) {
    return [value](bool /*closed*/) -> std::optional<int> { return value; };
}

static auto MakeDeclinedProducer() {
    return [](bool /*closed*/) -> std::optional<int> { return std::nullopt; };
}

TEST(BoundedMPMCQueue, EnqueueUpToCapacityBuffers) {
    bounded::AsyncMPMCQueue<int, 3> q;

    EXPECT_TRUE(q.Enqueue(MakeProducer(1)));
    EXPECT_TRUE(q.Enqueue(MakeProducer(2)));
    EXPECT_TRUE(q.Enqueue(MakeProducer(3)));

    std::vector<int> results;
    for (int i = 0; i < 3; ++i) {
        q.Dequeue([&](std::optional<int> v) {
            if (v) results.push_back(*v);
        });
    }

    ASSERT_EQ(results.size(), 3u);
    EXPECT_EQ(results[0], 1);
    EXPECT_EQ(results[1], 2);
    EXPECT_EQ(results[2], 3);
}

TEST(BoundedMPMCQueue, EnqueueBeyondCapacityQueuesCallback) {
    bounded::AsyncMPMCQueue<int, 2> q;

    q.Enqueue(MakeProducer(1));
    q.Enqueue(MakeProducer(2));

    // the queue is full; 
    // this callback should be queued, not called
    std::atomic_bool producerCalled{false};
    q.Enqueue([&](bool /*closed*/) -> std::optional<int> {
        producerCalled = true;
        return 3;
    });

    EXPECT_FALSE(producerCalled.load()) << "Queued producer should not be called until free slot available";

    std::optional<int> received;
    q.Dequeue([&](std::optional<int> v) { received = v; });

    EXPECT_TRUE(WaitFor([&] { return producerCalled.load(); }))
        << "Queued producer should fire once a slot becomes available";
    // The dequeue gets the oldest buffered value (1); producer(3) fills the freed slot.
    EXPECT_TRUE(received.has_value());
    EXPECT_EQ(*received, 1);
}

TEST(BoundedMPMCQueue, FIFOOrderWithOverflow) {
    constexpr int cap = 3;
    constexpr int total = 6;
    bounded::AsyncMPMCQueue<int, cap> q;

    for (int i = 0; i < total; ++i)
        q.Enqueue(MakeProducer(i));

    std::vector<int> results;
    std::mutex m;
    for (int i = 0; i < total; ++i) {
        q.Dequeue([&](std::optional<int> v) {
            std::lock_guard guard{m};
            if (v) results.push_back(*v);
        });
    }

    EXPECT_TRUE(WaitFor([&] {
        std::lock_guard guard{m};
        return static_cast<int>(results.size()) == total;
    }));

    std::lock_guard guard{m};
    for (int i = 0; i < total; ++i)
        EXPECT_EQ(results[i], i) << "FIFO order violated at index " << i;
}

TEST(BoundedMPMCQueue, ProducerDeclinesDoesNotCrash) {
    bounded::AsyncMPMCQueue<int, 2> q;
    EXPECT_TRUE(q.Enqueue(MakeDeclinedProducer()));

    // Queue should still be empty; a subsequent dequeue should not fire immediately.
    bool called = false;
    q.Dequeue([&](std::optional<int>) { called = true; });
    std::this_thread::sleep_for(10ms);
    EXPECT_FALSE(called) << "Declined producer should leave the queue empty";
    q.Close();
}

TEST(BoundedMPMCQueue, QueuedProducerDeclinesSlotStaysEmpty) {
    bounded::AsyncMPMCQueue<int, 1> q;

    q.Enqueue(MakeProducer(42));

    std::atomic_bool producerCalled{false};
    q.Enqueue([&](bool /*closed*/) -> std::optional<int> {
        producerCalled = true;
        return std::nullopt; // declines
    });

    std::optional<int> first;
    q.Dequeue([&](std::optional<int> v) { first = v; });

    EXPECT_TRUE(WaitFor([&] { return producerCalled.load(); }));
    EXPECT_TRUE(first.has_value());
    EXPECT_EQ(*first, 42);

    bool secondCalled = false;
    q.Dequeue([&](std::optional<int>) { secondCalled = true; });
    std::this_thread::sleep_for(10ms);
    EXPECT_FALSE(secondCalled) << "After declined producer the queue should be empty";
    q.Close();
}

TEST(BoundedMPMCQueue, EnqueueToClosedReturnsFalse) {
    bounded::AsyncMPMCQueue<int, 4> q;
    q.Close();
    EXPECT_FALSE(q.Enqueue(MakeProducer(1)));
}

TEST(BoundedMPMCQueue, DequeueFromClosedReceivesNullopt) {
    bounded::AsyncMPMCQueue<int, 4> q;
    q.Close();

    std::optional<int> result = 42;
    q.Dequeue([&](std::optional<int> v) { result = v; });
    EXPECT_FALSE(result.has_value());
}

TEST(BoundedMPMCQueue, CloseFlushesPendingDequeueCallbacks) {
    std::atomic_int nulloptCount{0};
    {
        bounded::AsyncMPMCQueue<int, 4> q;
        for (int i = 0; i < 5; ++i) {
            q.Dequeue([&](std::optional<int> v) {
                if (!v.has_value()) ++nulloptCount;
            });
        }
        q.Close();
    }
    EXPECT_EQ(nulloptCount.load(), 5);
}

TEST(BoundedMPMCQueue, CloseFlushesPendingEnqueueCallbacks) {
    std::atomic_int closedCount{0};
    {
        bounded::AsyncMPMCQueue<int, 1> q;
        q.Enqueue(MakeProducer(0));

        for (int i = 0; i < 3; ++i) {
            q.Enqueue([&](bool closed) -> std::optional<int> {
                if (closed) ++closedCount;
                return std::nullopt;
            });
        }
        q.Close();
    }
    EXPECT_EQ(closedCount.load(), 3) << "All queued producers should be called with closed=true on Close()";
}

TEST(BoundedMPMCQueue, DoubleCloseIsSafe) {
    bounded::AsyncMPMCQueue<int, 4> q;
    q.Close();
    EXPECT_NO_THROW(q.Close());
}

TEST(BoundedMPMCQueue, DestructorFlushesPendingCallbacks) {
    std::atomic_int nulloptCount{0};
    {
        bounded::AsyncMPMCQueue<int, 4> q;
        for (int i = 0; i < 3; ++i) {
            q.Dequeue([&](std::optional<int> v) {
                if (!v.has_value()) ++nulloptCount;
            });
        }
    }
    EXPECT_EQ(nulloptCount.load(), 3);
}

TEST(BoundedMPMCQueue, DirectCallOfWaitingConsumer) {
    bounded::AsyncMPMCQueue<int, 4> q;

    std::atomic_bool called{false};
    std::atomic_int result{0};
    q.Dequeue([&](std::optional<int> v) {
        if (v) { result = *v; called = true; }
    });

    q.Enqueue(MakeProducer(77));

    EXPECT_TRUE(WaitFor([&] { return called.load(); }));
    EXPECT_EQ(result.load(), 77);
}

TEST(BoundedMPMCQueue, SPSC) {
    constexpr int cap = 8;
    constexpr int N = 500;
    bounded::AsyncMPMCQueue<int, cap> q;

    std::atomic_int received{0};

    std::thread consumer([&] {
        for (int i = 0; i < N; ++i)
            q.Dequeue([&](std::optional<int> v) { if (v) ++received; });
    });

    std::thread producer([&] {
        for (int i = 0; i < N; ++i)
            q.Enqueue(MakeProducer(i));
    });

    producer.join();
    consumer.join();
    EXPECT_EQ(received.load(), N);
    q.Close();
}

TEST(BoundedMPMCQueue, MPSC) {
    constexpr int cap = 8;
    constexpr int producers = 4;
    constexpr int perProducer = 100;
    constexpr int total = producers * perProducer;
    bounded::AsyncMPMCQueue<int, cap> q;

    std::atomic_int received{0};

    std::thread consumer([&] {
        for (int i = 0; i < total; ++i)
            q.Dequeue([&](std::optional<int> v) { if (v) ++received; });
    });

    std::vector<std::thread> prodThreads;
    for (int p = 0; p < producers; ++p) {
        prodThreads.emplace_back([&, p] {
            for (int i = 0; i < perProducer; ++i)
                q.Enqueue(MakeProducer(p * perProducer + i));
        });
    }

    for (auto& t : prodThreads) t.join();
    consumer.join();
    EXPECT_EQ(received.load(), total);
    q.Close();
}

TEST(BoundedMPMCQueue, SPMC) {
    constexpr int cap = 8;
    constexpr int consumers = 4;
    constexpr int total = 400;
    bounded::AsyncMPMCQueue<int, cap> q;

    std::atomic_int received{0};

    std::vector<std::thread> consThreads;
    for (int c = 0; c < consumers; ++c) {
        consThreads.emplace_back([&] {
            for (int i = 0; i < total / consumers; ++i)
                q.Dequeue([&](std::optional<int> v) { if (v) ++received; });
        });
    }

    std::thread producer([&] {
        for (int i = 0; i < total; ++i)
            q.Enqueue(MakeProducer(i));
    });

    producer.join();
    for (auto& t : consThreads) t.join();
    EXPECT_EQ(received.load(), total);
    q.Close();
}

TEST(BoundedMPMCQueue, ThrowingEnqueueCallbackWithCapacity) {
    bounded::AsyncMPMCQueue<int, 4> q;

    EXPECT_NO_THROW(q.Enqueue([](bool) -> std::optional<int> {
        throw std::runtime_error("producer error");
    }));

    std::optional<int> result;
    q.Enqueue(MakeProducer(42));
    q.Dequeue([&](std::optional<int> v) { result = v; });

    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(*result, 42);
}

TEST(BoundedMPMCQueue, ThrowingEnqueueCallbackWithWaitingConsumer) {
    bounded::AsyncMPMCQueue<int, 4> q;

    std::atomic_bool consumerCalled{false};
    q.Dequeue([&](std::optional<int>) { consumerCalled = true; });

    EXPECT_NO_THROW(q.Enqueue([](bool) -> std::optional<int> {
        throw std::runtime_error("producer error");
    }));

    std::optional<int> result;
    q.Dequeue([&](std::optional<int> v) { result = v; });
    q.Enqueue(MakeProducer(7));

    EXPECT_TRUE(WaitFor([&] { return result.has_value(); }));
    EXPECT_EQ(*result, 7);
}

TEST(BoundedMPMCQueue, ThrowingQueuedEnqueueCallback) {
    bounded::AsyncMPMCQueue<int, 1> q;
    q.Enqueue(MakeProducer(1));

    q.Enqueue([](bool) -> std::optional<int> {
        throw std::runtime_error("queued producer error");
    });

    std::optional<int> first;
    EXPECT_NO_THROW(q.Dequeue([&](std::optional<int> v) { first = v; }));
    EXPECT_TRUE(first.has_value());
    EXPECT_EQ(*first, 1);

    std::optional<int> second;
    q.Enqueue(MakeProducer(99));
    q.Dequeue([&](std::optional<int> v) { second = v; });

    EXPECT_TRUE(WaitFor([&] { return second.has_value(); }));
    EXPECT_EQ(*second, 99);
}

TEST(BoundedMPMCQueue, ThrowingDequeueCallback) {
    bounded::AsyncMPMCQueue<int, 4> q;
    q.Enqueue(MakeProducer(5));

    EXPECT_NO_THROW(q.Dequeue([](std::optional<int>) {
        throw std::runtime_error("consumer error");
    }));

    std::optional<int> result;
    q.Enqueue(MakeProducer(10));
    q.Dequeue([&](std::optional<int> v) { result = v; });

    EXPECT_TRUE(WaitFor([&] { return result.has_value(); }));
    EXPECT_EQ(*result, 10);
}
