#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <queue>

namespace unbounded::basic {

template <typename T>
class AsyncMPMCQueue {
public:
    AsyncMPMCQueue() = default;

    bool Enqueue(T value) {
        std::unique_lock lock{mutex_};
        if (isClosed_) return false;

        valuesQueue_.push(std::move(value));
        if (!callbacksQueue_.empty()) {
            auto execCb = std::move(callbacksQueue_.front());
            callbacksQueue_.pop();

            auto val = std::move(valuesQueue_.front());
            valuesQueue_.pop();

            lock.unlock();

            try { execCb(std::move(val)); } catch (...) {}
        }
        return true;
    }

    using DequeueCallback = std::function<void(std::optional<T>)>;
    void Dequeue(DequeueCallback cb) {
        std::unique_lock lock{mutex_};
        callbacksQueue_.push(std::move(cb));

        if (!valuesQueue_.empty() || isClosed_) {
            auto execCb = std::move(callbacksQueue_.front());
            callbacksQueue_.pop();

            std::optional<T> val = std::nullopt;
            if (!valuesQueue_.empty()) {
                val = std::move(valuesQueue_.front());
                valuesQueue_.pop();
            } 
            lock.unlock();

            try { execCb(std::move(val)); } catch (...) {}
        } 
    }
    
    void Close() {
        std::unique_lock lock{mutex_};
        if (isClosed_) return;

        isClosed_ = true;
        while (!callbacksQueue_.empty()) {
            auto execCb = std::move(callbacksQueue_.front());
            callbacksQueue_.pop();

            lock.unlock();
            try { execCb(std::nullopt); } catch (...) {}
            lock.lock();
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

} // namespace unbounded::basic