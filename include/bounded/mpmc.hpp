#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <queue>

namespace bounded {

template <typename T>
class AsyncMPMCQueue {
public:
    AsyncMPMCQueue() = default;

    bool Enqueue(T value) {
        std::unique_lock lock{mutex_};
        if (isClosed_) return false;

        if (!callbacksQueue_.empty()) {
            auto execCb = std::move(callbacksQueue_.front());
            callbacksQueue_.pop();
            lock.unlock();
            try { execCb(std::move(value)); } catch (...) {}
        } else {
            valuesQueue_.push(std::move(value));
        }
        return true;
    }

    using DequeueCallback = std::function<void(std::optional<T>)>;
    void Dequeue(DequeueCallback cb) {
        std::unique_lock lock{mutex_};

        if (!valuesQueue_.empty() || isClosed_) {
            std::optional<T> val = std::nullopt;
            if (!valuesQueue_.empty()) {
                val = std::move(valuesQueue_.front());
                valuesQueue_.pop();
            } 
            lock.unlock();

            try { cb(std::move(val)); } catch (...) {}
        } else {
            callbacksQueue_.push(std::move(cb));
        } 
    }
    
    void Close() {
        std::unique_lock lock{mutex_};
        if (isClosed_) return;

        std::queue<DequeueCallback> localCallbacks;
        callbacksQueue_.swap(localCallbacks);
        isClosed_ = true;
        lock.unlock();

        while (!localCallbacks.empty()) {
            auto execCb = std::move(localCallbacks.front());
            localCallbacks.pop();
            try { execCb(std::nullopt); } catch (...) {}
        }
    }

    ~AsyncMPMCQueue() {
        Close();
    }

private:
    std::mutex mutex_;
    bool isClosed_{false};

    std::queue<T> valuesQueue_;
    std::queue<DequeueCallback> callbacksQueue_;
};

} // namespace bounded