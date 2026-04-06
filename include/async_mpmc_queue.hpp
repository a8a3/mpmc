#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

namespace unbounded {

template <typename T>
class AsyncMPMCQueue {
public:
    AsyncMPMCQueue() {
        workerThread_ = std::thread(&AsyncMPMCQueue::workerThreadFunc, this);
    }

    bool Enqueue(T value) {
        std::unique_lock lock{mutex_};
        if (isClosed_) return false;

        valuesQueue_.push(std::move(value));
        if (!callbacksQueue_.empty()) {
            lock.unlock();
            cv_.notify_one();
        }
        return true;
    }

    using DequeueCallback = std::function<void(std::optional<T>)>;
    void Dequeue(DequeueCallback cb) {
        std::unique_lock lock{mutex_};

        if (isClosed_) {
            lock.unlock();
            try { cb(std::nullopt); } catch (...) {}
            return;
        }

        callbacksQueue_.push(std::move(cb));
        if (!valuesQueue_.empty()) {
            lock.unlock();
            cv_.notify_one();
        }
    }
    
    void Close() {
        {
            std::unique_lock lock{mutex_};
            if (isClosed_) return;
            isClosed_ = true;
        }
        cv_.notify_one();
    }

    ~AsyncMPMCQueue() {
        Close();
        if (workerThread_.joinable())
            workerThread_.join();
    }

private:

    void workerThreadFunc() {
        while (true) {
            std::unique_lock lock{mutex_};
            cv_.wait(lock, [this] {
                return isClosed_ || (!valuesQueue_.empty() && !callbacksQueue_.empty());
            });

            if (isClosed_) break;  // while

            while (!valuesQueue_.empty() && !callbacksQueue_.empty()) {
                auto value = std::move(valuesQueue_.front());
                valuesQueue_.pop();

                auto cb = std::move(callbacksQueue_.front());
                callbacksQueue_.pop();

                lock.unlock();
                try { cb(std::move(value)); } catch (...) {}
                lock.lock();
                if (isClosed_) break;  // вызов Close() мог произойти во время выполнения callback-а
            }
        }

        std::unique_lock lock{mutex_};
        while (!callbacksQueue_.empty()) {
            auto cb = std::move(callbacksQueue_.front());
            callbacksQueue_.pop();

            lock.unlock();
            try { cb(std::nullopt); } catch (...) {}
            lock.lock();
        }
    }

    std::mutex mutex_;
    std::condition_variable cv_;

    std::queue<T> valuesQueue_;
    std::queue<DequeueCallback> callbacksQueue_;
    
    std::thread workerThread_;
    bool isClosed_ = false;
};

} // namespace unbounded