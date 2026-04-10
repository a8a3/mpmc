#include <gtest/gtest.h>
#include "mpmc_basic.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

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

using QueueTypes = ::testing::Types<
    unbounded::basic::AsyncMPMCQueue<int>
>;

template <typename Q>
class AsyncMPMCQueueTypedTest : public ::testing::Test {};

TYPED_TEST_SUITE(AsyncMPMCQueueTypedTest, QueueTypes);

TYPED_TEST(AsyncMPMCQueueTypedTest, EnqueueToClosed) {
    TypeParam q;
    q.Close();
    EXPECT_FALSE(q.Enqueue(1)) << "Enqueue to closed queue should return false";
}

TYPED_TEST(AsyncMPMCQueueTypedTest, DequeueFromClosed) {
    TypeParam q;
    q.Close();

    std::optional<int> result = 42;
    q.Dequeue([&](std::optional<int> v) { result = v; });
    EXPECT_FALSE(result.has_value()) << "Expected nullopt, got " << *result;
}

TYPED_TEST(AsyncMPMCQueueTypedTest, CloseFlushPendingCallbacks) {
    std::atomic_int nullopt_count{0};
    {
        TypeParam q;
        for (int i = 0; i < 5; ++i) {
            q.Dequeue([&](std::optional<int> v) {
                if (!v.has_value())
                    ++nullopt_count;
            });
        }
        q.Close();
    }
    EXPECT_EQ(nullopt_count.load(), 5) << "All pending callbacks should be called with nullopt on Close()";
}

TYPED_TEST(AsyncMPMCQueueTypedTest, DoubleClose) {
    TypeParam q;
    q.Close();
    EXPECT_NO_THROW(q.Close()) << "Second Close() should be safe";
}

TYPED_TEST(AsyncMPMCQueueTypedTest, DestructorFlushPendingCallbacks) {
    std::atomic_int nullopt_count{0};
    {
        TypeParam q;
        for (int i = 0; i < 3; ++i) {
            q.Dequeue([&](std::optional<int> v) {
                if (!v.has_value())
                    ++nullopt_count;
            });
        }
    }
    EXPECT_EQ(nullopt_count.load(), 3) << "Destructor should call all pending callbacks with nullopt";
}

TYPED_TEST(AsyncMPMCQueueTypedTest, EnqueueThenDequeue) {
    TypeParam q;

    std::atomic_bool called{false};
    std::atomic_int result{0};
    q.Enqueue(42);
    q.Dequeue([&](std::optional<int> v) { result = *v; called = true; });

    EXPECT_TRUE(WaitFor([&] { return called.load(); })) << "Callback should be called with enqueued value";
    EXPECT_EQ(result.load(), 42) << "Callback should receive the enqueued value";
}

TYPED_TEST(AsyncMPMCQueueTypedTest, DequeueThenEnqueue) {
    TypeParam q;

    std::atomic_bool called{false};
    std::atomic_int result{0};
    q.Dequeue([&](std::optional<int> v) { result = *v; called = true; });
    q.Enqueue(99);

    EXPECT_TRUE(WaitFor([&] { return called.load(); })) << "Pending callback should be called after Enqueue";
    EXPECT_EQ(result.load(), 99) << "Callback should receive the value enqueued after it was registered";
}

TYPED_TEST(AsyncMPMCQueueTypedTest, MultipleEnqueueDequeue) {
    TypeParam q;
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

TYPED_TEST(AsyncMPMCQueueTypedTest, FIFOOrder) {
    TypeParam q;
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

TYPED_TEST(AsyncMPMCQueueTypedTest, SPSC) {
    TypeParam q;
    const int N = 1000;

    std::atomic_int received{0};

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

TYPED_TEST(AsyncMPMCQueueTypedTest, MPSC) {
    TypeParam q;
    const int producers = 8;
    const int per_producer = 100;
    const int total = producers * per_producer;

    std::atomic_int received{0};

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

TYPED_TEST(AsyncMPMCQueueTypedTest, SPMC) {
    TypeParam q;
    const int consumers = 8;
    const int total = 800;

    std::atomic_int received{0};

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

TYPED_TEST(AsyncMPMCQueueTypedTest, MPMC) {
    TypeParam q;
    const int producers = 4;
    const int consumers = 4;
    const int per_producer = 250;
    const int total = producers * per_producer;

    std::atomic_int received{0};

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

TYPED_TEST(AsyncMPMCQueueTypedTest, CloseDuringActivity) {
    std::atomic_int values_received{0};
    std::atomic_int nullopts_received{0};

    {
        TypeParam q;
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
    }

    EXPECT_EQ(values_received.load() + nullopts_received.load(), 200)
        << "Every callback must be called exactly once (either with value or nullopt), got "
        << values_received.load() << " values and " << nullopts_received.load() << " nullopts";
}

TYPED_TEST(AsyncMPMCQueueTypedTest, ReentrantEnqueueFromCallback) {
    TypeParam q;

    std::atomic_int received{0};
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

TYPED_TEST(AsyncMPMCQueueTypedTest, ThrowingCallbackDoesNotBreakQueue) {
    TypeParam q;

    std::atomic_int received{0};

    q.Enqueue(1);
    q.Enqueue(2);
    q.Enqueue(3);

    q.Dequeue([&](std::optional<int>) { throw std::runtime_error("error"); });
    q.Dequeue([&](std::optional<int> v) { if (v) ++received; });
    q.Dequeue([&](std::optional<int> v) { if (v) ++received; });

    EXPECT_TRUE(WaitFor([&] { return received.load() == 2; })) << "Queue should remain functional after a callback throws";
    q.Close();
}

TYPED_TEST(AsyncMPMCQueueTypedTest, DequeueDrainsValuesAfterClose) {
    TypeParam q;
    const int N = 5;

    for (int i = 0; i < N; ++i)
        q.Enqueue(i);
    q.Close();

    std::vector<int> values;
    std::atomic_int nullopt_count{0};
    std::mutex m;

    // N dequeues should drain existing values, then one more should get nullopt
    for (int i = 0; i < N + 1; ++i) {
        q.Dequeue([&](std::optional<int> v) {
            if (v) {
                std::lock_guard guard{m};
                values.push_back(*v);
            } else {
                ++nullopt_count;
            }
        });
    }

    EXPECT_TRUE(WaitFor([&] {
        std::lock_guard guard{m};
        return (int)values.size() + nullopt_count.load() == N + 1;
    })) << "All callbacks should be called";

    std::lock_guard guard{m};
    EXPECT_EQ((int)values.size(), N) << "All enqueued values should be delivered after Close()";
    EXPECT_EQ(nullopt_count.load(), 1) << "Only the last Dequeue (past existing values) should get nullopt";
    for (int i = 0; i < N; ++i)
        EXPECT_EQ(values[i], i) << "Values should be delivered in FIFO order after Close(), index " << i;
}

TYPED_TEST(AsyncMPMCQueueTypedTest, CloseFromCallback) {
    {
        TypeParam q;
        std::atomic_bool close_called{false};
        q.Enqueue(1);
        q.Dequeue([&](std::optional<int>) {
            q.Close();
            close_called = true;
        });

        EXPECT_TRUE(WaitFor([&] { return close_called.load(); })) << "Callback should have been called";
    }
}

TYPED_TEST(AsyncMPMCQueueTypedTest, LongRunningCallback) {
    TypeParam q;

    std::atomic_int done{0};
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
