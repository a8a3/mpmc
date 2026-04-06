#include <gtest/gtest.h>
#include "async_mpmc_queue.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace unbounded;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

template <typename Pred>
bool WaitFor(Pred pred, std::chrono::milliseconds timeout = 1'000ms) {
    auto deadline = Clock::now() + timeout;
    while (!pred()) {
        if (Clock::now() > deadline) 
            return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

TEST(AsyncMPMCQueueTest, EnqueueToClosed) {
    AsyncMPMCQueue<int> q;
    q.Close();
    EXPECT_FALSE(q.Enqueue(1)) << "Enqueue to closed queue should return false";
}

TEST(AsyncMPMCQueueTest, DequeueFromClosed) {
    AsyncMPMCQueue<int> q;
    q.Close();

    std::optional<int> result = 42;
    q.Dequeue([&](std::optional<int> v) { result = v; });
    EXPECT_FALSE(result.has_value()) << "Expected nullopt, got " << *result;
}

TEST(AsyncMPMCQueueTest, CloseFlushPendingCallbacks) {
    std::atomic<int> nullopt_count{0};
    {
        AsyncMPMCQueue<int> q;
        for (int i = 0; i < 5; ++i) {
            q.Dequeue([&](std::optional<int> v) {
                if (!v.has_value())
                    ++nullopt_count;
            });
        }
        q.Close();
    } // destructor joins worker, all callbacks guaranteed to have run
    EXPECT_EQ(nullopt_count.load(), 5) << "All pending callbacks should be called with nullopt on Close()";
}

TEST(AsyncMPMCQueueTest, DoubleClose) {
    AsyncMPMCQueue<int> q;
    q.Close();
    EXPECT_NO_THROW(q.Close()) << "Second Close() should be safe";
}

TEST(AsyncMPMCQueueTest, DestructorFlushPendingCallbacks) {
    std::atomic<int> nullopt_count{0};
    {
        AsyncMPMCQueue<int> q;
        for (int i = 0; i < 3; ++i) {
            q.Dequeue([&](std::optional<int> v) {
                if (!v.has_value()) 
                    ++nullopt_count;
            });
        }
    }
    EXPECT_EQ(nullopt_count.load(), 3) << "Destructor should call all pending callbacks with nullopt";
}

TEST(AsyncMPMCQueueTest, EnqueueThenDequeue) {
    AsyncMPMCQueue<int> q;

    std::atomic<bool> called{false};
    std::atomic<int> result{0};
    q.Enqueue(42);
    q.Dequeue([&](std::optional<int> v) { result = *v; called = true; });

    EXPECT_TRUE(WaitFor([&] { return called.load(); })) << "Callback should be called with enqueued value";
    EXPECT_EQ(result.load(), 42) << "Callback should receive the enqueued value";
}

TEST(AsyncMPMCQueueTest, DequeueThenEnqueue) {
    AsyncMPMCQueue<int> q;

    std::atomic<bool> called{false};
    std::atomic<int> result{0};
    q.Dequeue([&](std::optional<int> v) { result = *v; called = true; });
    q.Enqueue(99);

    EXPECT_TRUE(WaitFor([&] { return called.load(); })) << "Pending callback should be called after Enqueue";
    EXPECT_EQ(result.load(), 99) << "Callback should receive the value enqueued after it was registered";
}

TEST(AsyncMPMCQueueTest, MultipleEnqueueDequeue) {
    AsyncMPMCQueue<int> q;
    const int N = 100;

    std::vector<int> results;
    std::mutex m;

    for (int i = 0; i < N; ++i) q.Enqueue(i);
    for (int i = 0; i < N; ++i) {
        q.Dequeue([&](std::optional<int> v) {
            std::lock_guard guard{m};
            if (v) 
                results.push_back(*v);
        });
    }

    EXPECT_TRUE(WaitFor([&] {
        std::lock_guard guard{m};
        return static_cast<int>(results.size()) == N;
    })) << "All " << N << " callbacks should be called";

    std::lock_guard guard{m};
    for (int i = 0; i < N; ++i)
        EXPECT_EQ(results[i], i) << "Every enqueued value should be received exactly once";
}

TEST(AsyncMPMCQueueTest, FIFOOrder) {
    AsyncMPMCQueue<int> q;
    const int N = 50;

    std::vector<int> results;
    std::mutex m;

    for (int i = 0; i < N; ++i) 
        q.Enqueue(i);

    for (int i = 0; i < N; ++i) {
        q.Dequeue([&](std::optional<int> val) {
            std::lock_guard guard{m};
            if (val) 
                results.push_back(*val);
        });
    }

    EXPECT_TRUE(WaitFor([&] {
        std::lock_guard guard{m};
        return (int)results.size() == N;
    })) << "All " << N << " callbacks should be called";

    std::lock_guard guard{m};
    for (int i = 0; i < N; ++i) EXPECT_EQ(results[i], i) << "Values should be dequeued in FIFO order, index " << i;
}

TEST(AsyncMPMCQueueTest, SPSC) {
    AsyncMPMCQueue<int> q;
    const int N = 1000;

    std::atomic<int> received{0};

    std::thread consumer([&] {
        for (int i = 0; i < N; ++i) {
            q.Dequeue([&](std::optional<int> v) {
                if (v) 
                    ++received;
            });
        }
    });

    std::thread producer([&] {
        for (int i = 0; i < N; ++i) q.Enqueue(i);
    });

    producer.join();
    consumer.join();

    EXPECT_TRUE(WaitFor([&] { return received.load() == N; })) << "All " << N << " values should be received";
    q.Close();
}

TEST(AsyncMPMCQueueTest, MPSC) {
    AsyncMPMCQueue<int> q;
    const int producers = 8;
    const int per_producer = 100;
    const int total = producers * per_producer;

    std::atomic<int> received{0};

    std::thread consumer([&] {
        for (int i = 0; i < total; ++i) {
            q.Dequeue([&](std::optional<int> v) {
                if (v) 
                    ++received;
            });
        }
    });

    std::vector<std::thread> prod_threads;
    for (int p = 0; p < producers; ++p) {
        prod_threads.emplace_back([&, p] {
            for (int i = 0; i < per_producer; ++i)
                q.Enqueue(p * per_producer + i);
        });
    }

    for (auto& t : prod_threads) t.join();
    consumer.join();

    EXPECT_TRUE(WaitFor([&] { return received.load() == total; })) << "All " << total << " values should be received across " << producers << " producers";
    q.Close();
}

TEST(AsyncMPMCQueueTest, SPMC) {
    AsyncMPMCQueue<int> q;
    const int consumers = 8;
    const int total = 800;

    std::atomic<int> received{0};

    std::vector<std::thread> cons_threads;
    for (int c = 0; c < consumers; ++c) {
        cons_threads.emplace_back([&] {
            for (int i = 0; i < total / consumers; ++i) {
                q.Dequeue([&](std::optional<int> v) {
                    if (v) 
                        ++received;
                });
            }
        });
    }

    std::thread producer([&] {
        for (int i = 0; i < total; ++i) q.Enqueue(i);
    });

    producer.join();
    for (auto& t : cons_threads) t.join();

    EXPECT_TRUE(WaitFor([&] { return received.load() == total; })) << "All " << total << " values should be received across " << consumers << " consumers";
    q.Close();
}

TEST(AsyncMPMCQueueTest, MPMC) {
    AsyncMPMCQueue<int> q;
    const int producers = 4;
    const int consumers = 4;
    const int per_producer = 250;
    const int total = producers * per_producer;

    std::atomic<int> received{0};

    std::vector<std::thread> cons_threads;
    for (int c = 0; c < consumers; ++c) {
        cons_threads.emplace_back([&] {
            for (int i = 0; i < total / consumers; ++i) {
                q.Dequeue([&](std::optional<int> v) {
                    if (v) 
                        ++received;
                });
            }
        });
    }

    std::vector<std::thread> prod_threads;
    for (int p = 0; p < producers; ++p) {
        prod_threads.emplace_back([&, p] {
            for (int i = 0; i < per_producer; ++i)
                q.Enqueue(p * per_producer + i);
        });
    }

    for (auto& t : prod_threads) t.join();
    for (auto& t : cons_threads) t.join();

    EXPECT_TRUE(WaitFor([&] { return received.load() == total; })) << "All " << total << " values should be received with " << producers << " producers and " << consumers << " consumers";
    q.Close();
}

TEST(AsyncMPMCQueueTest, CloseDuringActivity) {
    std::atomic<int> values_received{0};
    std::atomic<int> nullopts_received{0};

    {
        AsyncMPMCQueue<int> q;
        const int N = 200;

        std::thread consumer([&] {
            for (int i = 0; i < N; ++i) {
                q.Dequeue([&](std::optional<int> v) {
                    if (v)
                        ++values_received;
                    else
                        ++nullopts_received;
                });
            }
        });

        std::thread producer([&] {
            for (int i = 0; i < N / 2; ++i)
                q.Enqueue(i);
        });

        std::this_thread::sleep_for(5ms);
        q.Close();

        producer.join();
        consumer.join();
    } // destructor joins worker, all callbacks guaranteed to have run

    EXPECT_EQ(values_received.load() + nullopts_received.load(), 200)
        << "Every callback must be called exactly once (either with value or nullopt), got "
        << values_received.load() << " values and " << nullopts_received.load() << " nullopts";
}

TEST(AsyncMPMCQueueTest, MoveOnlyType) {
    AsyncMPMCQueue<std::unique_ptr<int>> q;

    q.Enqueue(std::make_unique<int>(7));

    std::atomic_bool called{false};
    std::unique_ptr<int> result;
    q.Dequeue([&](std::optional<std::unique_ptr<int>> v) {
        result = std::move(*v);
        called.store(true);
    });

    EXPECT_TRUE(WaitFor([&] { return called.load(); })) << "Callback should be called for move-only type";
    EXPECT_EQ(*result, 7) << "Callback should receive the correct value for move-only type";
    q.Close();
}

TEST(AsyncMPMCQueueTest, LongRunningCallback) {
    AsyncMPMCQueue<int> q;

    std::atomic<int> done{0};
    for (int i = 0; i < 4; ++i) {
        q.Enqueue(i);
        q.Dequeue([&](std::optional<int>) {
            std::this_thread::sleep_for(1'000ms);
            ++done;
        });
    }

    EXPECT_TRUE(WaitFor([&] { return done.load() == 4; }, 5'000ms)) << "Queue should not deadlock with long-running callbacks";
    q.Close();
}

TEST(AsyncMPMCQueueTest, ReentrantEnqueueFromCallback) {
    AsyncMPMCQueue<int> q;

    std::atomic<int> received{0};
    q.Enqueue(1);
    q.Dequeue([&](std::optional<int> v) {
        if (v) {
            ++received;
            q.Enqueue(2);
        }
    });
    q.Dequeue([&](std::optional<int> v) {
        if (v) ++received;
    });

    EXPECT_TRUE(WaitFor([&] { return received.load() == 2; })) << "Re-entrant Enqueue from callback should not deadlock";
    q.Close();
}

TEST(AsyncMPMCQueueTest, ThrowingCallbackDoesNotBreakQueue) {
    AsyncMPMCQueue<int> q;

    std::atomic<int> received{0};

    q.Enqueue(1);
    q.Enqueue(2);
    q.Enqueue(3);

    q.Dequeue([&](std::optional<int>) { throw std::runtime_error("error"); });
    q.Dequeue([&](std::optional<int> v) { if (v) ++received; });
    q.Dequeue([&](std::optional<int> v) { if (v) ++received; });

    EXPECT_TRUE(WaitFor([&] { return received.load() == 2; })) << "Queue should remain functional after a callback throws";
    q.Close();
}

TEST(AsyncMPMCQueueTest, CloseFromCallback) {
    std::atomic<bool> close_called{false};

    {
        AsyncMPMCQueue<int> q;
        q.Enqueue(1);
        q.Dequeue([&](std::optional<int>) {
            q.Close();
            close_called = true;
        });

        EXPECT_TRUE(WaitFor([&] { return close_called.load(); })) << "Callback should have been called";
    } // destructor joins worker — no deadlock

    EXPECT_TRUE(close_called.load()) << "Close() from inside callback should not deadlock or crash";
}
