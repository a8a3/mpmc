#include <gtest/gtest.h>
#include "cancellable/mpmc.hpp"

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Queue = cancellable::AsyncMPMCQueue<int>;

template <typename Pred>
static bool WaitFor(Pred pred, std::chrono::milliseconds timeout = 1'000ms) {
    auto deadline = Clock::now() + timeout;
    while (!pred()) {
        if (Clock::now() > deadline) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

TEST(CancellableQueueCancelTest, PendingDequeueReturnsId) {
    Queue q;
    auto id = q.Dequeue([](std::optional<int>) {});
    EXPECT_TRUE(id.has_value()) << "Dequeue with no pending value must return a live CallbackId";
    q.Close();
}

TEST(CancellableQueueCancelTest, ImmediateDequeueReturnsNullopt) {
    Queue q;
    q.Enqueue(1);
    auto id = q.Dequeue([](std::optional<int>) {});
    EXPECT_FALSE(id.has_value()) << "Dequeue when a value is already queued must fire immediately and return nullopt";
    q.Close();
}

TEST(CancellableQueueCancelTest, DequeueOnClosedQueueReturnsNullopt) {
    Queue q;
    q.Close();
    auto id = q.Dequeue([](std::optional<int>) {});
    EXPECT_FALSE(id.has_value()) << "Dequeue on a closed queue must fire immediately and return nullopt";
}

TEST(CancellableQueueCancelTest, CancelReturnsTrueForPendingCallback) {
    Queue q;
    auto id = q.Dequeue([](std::optional<int>) {});
    ASSERT_TRUE(id.has_value()) << "Dequeue on empty queue must return a CallbackId";
    EXPECT_TRUE(q.Cancel(*id)) << "Cancel must return true for a live pending callback";
    q.Close();
}

TEST(CancellableQueueCancelTest, CancelledCallbackNeverFires) {
    Queue q;
    bool called = false;
    auto id = q.Dequeue([&](std::optional<int>) { called = true; });
    ASSERT_TRUE(id.has_value()) << "Dequeue on empty queue must return a CallbackId";
    q.Cancel(*id);
    q.Enqueue(1);
    std::this_thread::sleep_for(20ms);
    EXPECT_FALSE(called) << "Cancelled callback must never be invoked; value goes to the queue instead";
    q.Close();
}

TEST(CancellableQueueCancelTest, CancelFrontCallbackNextGetsValue) {
    Queue q;
    std::atomic_int received{-1};
    auto id0 = q.Dequeue([&](std::optional<int>) { received = 0; });
    auto id1 = q.Dequeue([&](std::optional<int> v) { if (v) received = *v; });
    ASSERT_TRUE(id0.has_value()) << "Dequeue on empty queue must return a CallbackId";
    ASSERT_TRUE(id1.has_value()) << "Dequeue on empty queue must return a CallbackId";
    EXPECT_TRUE(q.Cancel(*id0)) << "Cancel must return true for a live pending callback";
    q.Enqueue(42);
    EXPECT_TRUE(WaitFor([&] { return received.load() != -1; })) << "Next pending callback must receive the value after front was cancelled";
    EXPECT_EQ(received.load(), 42) << "id0 (front) must not fire; id1 must receive the enqueued value";
}

TEST(CancellableQueueCancelTest, CancelMiddleCallbackFIFOPreserved) {
    Queue q;
    std::vector<int> received;
    std::mutex m;
    auto good_cb = [&](std::optional<int> v) {
        if (v) { std::lock_guard g{m}; received.push_back(*v); }
    };
    auto id0 = q.Dequeue(good_cb);
    auto id1 = q.Dequeue([&](std::optional<int> v) {
        if (v) { std::lock_guard g{m}; received.push_back(-1); }
    });
    auto id2 = q.Dequeue(good_cb);
    ASSERT_TRUE(id1.has_value()) << "Dequeue on empty queue must return a CallbackId";
    EXPECT_TRUE(q.Cancel(*id1)) << "Cancel must return true for a live pending callback";
    q.Enqueue(10);
    q.Enqueue(20);
    EXPECT_TRUE(WaitFor([&] { std::lock_guard g{m}; return received.size() == 2; })) << "Remaining two callbacks (id0, id2) must each receive one value";
    std::lock_guard g{m};
    EXPECT_EQ(received[0], 10) << "id0 (front) must receive the first value; sentinel -1 from cancelled id1 must not appear";
    EXPECT_EQ(received[1], 20) << "id2 (back) must receive the second value; FIFO order must be preserved after middle cancellation";
}

TEST(CancellableQueueCancelTest, CancelLastCallbackFrontStillGetsValue) {
    Queue q;
    std::atomic_int received{-1};
    auto id0 = q.Dequeue([&](std::optional<int> v) { if (v) received = *v; });
    auto id1 = q.Dequeue([&](std::optional<int>) { received = -2; });
    ASSERT_TRUE(id1.has_value()) << "Dequeue on empty queue must return a CallbackId";
    EXPECT_TRUE(q.Cancel(*id1)) << "Cancel must return true for a live pending callback";
    q.Enqueue(7);
    EXPECT_TRUE(WaitFor([&] { return received.load() != -1; })) << "Front callback must receive the value after back was cancelled";
    EXPECT_EQ(received.load(), 7) << "id1 (back) must not fire; id0 must receive the enqueued value";
}

TEST(CancellableQueueCancelTest, CancelAllPendingQueueRemainsUsable) {
    Queue q;
    bool any_called = false;
    std::vector<std::optional<Queue::CallbackId>> ids;
    for (int i = 0; i < 5; ++i)
        ids.push_back(q.Dequeue([&](std::optional<int>) { any_called = true; }));
    for (auto& id : ids) {
        ASSERT_TRUE(id.has_value()) << "Dequeue on empty queue must return a CallbackId";
        EXPECT_TRUE(q.Cancel(*id)) << "Cancel must return true for each live pending callback";
    }
    q.Enqueue(99);
    std::atomic_bool got_value{false};
    q.Dequeue([&](std::optional<int> v) { if (v && *v == 99) got_value = true; });
    EXPECT_TRUE(WaitFor([&] { return got_value.load(); })) << "Enqueue must store the value when no pending callbacks remain after all were cancelled";
    EXPECT_FALSE(any_called) << "None of the cancelled callbacks must ever be invoked";
    q.Close();
}

TEST(CancellableQueueCancelTest, CancelFromInsideCallbackIsReentrantSafe) {
    Queue q;
    bool second_called = false;
    std::optional<Queue::CallbackId> id1;
    auto id0 = q.Dequeue([&](std::optional<int>) {
        if (id1.has_value()) q.Cancel(*id1);
    });
    id1 = q.Dequeue([&](std::optional<int>) { second_called = true; });
    ASSERT_TRUE(id0.has_value()) << "Dequeue on empty queue must return a CallbackId";
    ASSERT_TRUE(id1.has_value()) << "Dequeue on empty queue must return a CallbackId";
    q.Enqueue(1);
    q.Enqueue(2);
    std::this_thread::sleep_for(20ms);
    EXPECT_FALSE(second_called) << "Cancel called from inside a running callback must not deadlock and must prevent the second callback from firing";
    q.Close();
}

TEST(CancellableQueueCancelTest, CancelReturnsFalseWhenQueueIsClosed) {
    Queue q;
    auto id = q.Dequeue([](std::optional<int>) {});
    ASSERT_TRUE(id.has_value()) << "Dequeue on empty queue must return a CallbackId";
    q.Close();
    EXPECT_FALSE(q.Cancel(*id)) << "Cancel must return false when the queue is closed";
}

TEST(CancellableQueueCancelTest, CancelReturnsFalseForUnknownId) {
    Queue q;
    EXPECT_FALSE(q.Cancel(42)) << "Cancel must return false for an ID that was never issued";
    q.Close();
}

TEST(CancellableQueueCancelTest, CancelReturnsFalseAfterCallbackFiredByEnqueue) {
    Queue q;
    bool fired = false;
    auto id = q.Dequeue([&](std::optional<int>) { fired = true; });
    ASSERT_TRUE(id.has_value()) << "Dequeue on empty queue must return a CallbackId";
    q.Enqueue(1);
    EXPECT_TRUE(fired) << "Enqueue removes the ID from the map and calls the callback before returning";
    EXPECT_FALSE(q.Cancel(*id)) << "Cancel must return false after the callback was already dispatched by Enqueue";
    q.Close();
}

TEST(CancellableQueueCancelTest, CancelReturnsFalseOnDoubleCancellation) {
    Queue q;
    auto id = q.Dequeue([](std::optional<int>) {});
    ASSERT_TRUE(id.has_value()) << "Dequeue on empty queue must return a CallbackId";
    EXPECT_TRUE(q.Cancel(*id)) << "First Cancel must return true for a live pending callback";
    EXPECT_FALSE(q.Cancel(*id)) << "Second Cancel on the same ID must return false";
    q.Close();
}

TEST(CancellableQueueCancelTest, CancelReturnsFalseAfterCloseDrainedCallback) {
    Queue q;
    bool fired = false;
    auto id = q.Dequeue([&](std::optional<int>) { fired = true; });
    ASSERT_TRUE(id.has_value()) << "Dequeue on empty queue must return a CallbackId";
    q.Close();
    EXPECT_TRUE(fired) << "Close must call the pending callback with nullopt before returning";
    EXPECT_FALSE(q.Cancel(*id)) << "Cancel must return false after Close already drained the callback";
}

TEST(CancellableQueueCancelTest, ConcurrentCancelAndEnqueueExactlyOneWins) {
    const int iterations = 100;
    for (int i = 0; i < iterations; ++i) {
        Queue q;
        std::atomic_bool fired{false};
        auto id = q.Dequeue([&](std::optional<int> v) { if (v) fired = true; });
        ASSERT_TRUE(id.has_value()) << "Dequeue on empty queue must return a CallbackId";

        std::atomic_bool cancelled{false};
        std::thread canceller([&] { cancelled = q.Cancel(*id); });
        std::thread enqueuer([&] { q.Enqueue(1); });
        canceller.join();
        enqueuer.join();

        EXPECT_NE(cancelled.load(), fired.load())
            << "Exactly one of cancel or enqueue must win, iteration " << i;

        q.Close();
    }
}

TEST(CancellableQueueCancelTest, ConcurrentDistinctCancelsAllSucceed) {
    Queue q;
    const int N = 100;
    std::vector<std::optional<Queue::CallbackId>> ids;
    ids.reserve(N);
    for (int i = 0; i < N; ++i)
        ids.push_back(q.Dequeue([](std::optional<int>) {}));

    std::atomic_int cancel_count{0};
    std::vector<std::thread> threads;
    threads.reserve(N);
    for (int i = 0; i < N; ++i) {
        threads.emplace_back([&, i] {
            if (ids[i].has_value() && q.Cancel(*ids[i]))
                ++cancel_count;
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(cancel_count.load(), N) << "Each of the " << N << " threads must successfully cancel its own distinct pending callback";
    q.Close();
}
