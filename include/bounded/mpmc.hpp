#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <queue>

namespace bounded {

template <typename T, std::size_t TCapacity>
class AsyncMPMCQueue {
public:
    AsyncMPMCQueue() = default;

    // Превращение очереди в ограниченную (bounded).
    // В этом случае Enqueue тоже начинает принимать колбек
    // (using EnqueueCallback = std::function<std::optional<T>(bool /*closed*/)>;),
    // который будет вызван, когда в очереди появится свободный слот.
    using EnqueueCallback = std::function<std::optional<T>(bool/*closed*/)>;
    bool Enqueue(EnqueueCallback enqCb) {
        std::unique_lock lock{mutex_};

        if (!deqCbQueue_.empty()) {
            auto deqCb = std::move(deqCbQueue_.front());
            deqCbQueue_.pop();
            bool localIsClosed = isClosed_;
            lock.unlock();

            try { 
                auto maybeValue = enqCb(localIsClosed);
                deqCb(std::move(maybeValue)); 
            } catch (...) {}

        } else {

            if (isClosed_) return false;

            // если в очереди значений есть место-
            // вызвать переданный коллбек и положить в очередь значение, если вернется
            if (valuesQueue_.size() < TCapacity) {
                if (auto maybeValue = enqCb(isClosed_)) {
                    valuesQueue_.emplace(std::move(*maybeValue));
                }
            // если места нет-
            // поместить коллбек в очередь коллбеков- продьюсеров
            } else {
                enqCbQueue_.emplace(std::move(enqCb));
            }
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

                // Значение ушло из очереди, посмотреть- можно ли добавить из 
                // очереди ожидания
                if (!enqCbQueue_.empty()) {
                    auto enqCb = std::move(enqCbQueue_.front());
                    enqCbQueue_.pop();

                    if (auto maybeValue = enqCb(isClosed_)) {
                        valuesQueue_.emplace(std::move(*maybeValue));
                    }
                }
            } 
            lock.unlock();

            // Потребить значение
            try { cb(std::move(val)); } catch (...) {}
        } else {
            deqCbQueue_.push(std::move(cb));
        } 
    }
    
    void Close() {
        std::unique_lock lock{mutex_};
        if (isClosed_) return;

        std::queue<DequeueCallback> localDeqCallbacks;
        deqCbQueue_.swap(localDeqCallbacks);

        std::queue<EnqueueCallback> localEnqCallbacks;
        enqCbQueue_.swap(localEnqCallbacks);

        isClosed_ = true;
        lock.unlock();

        while (!localDeqCallbacks.empty()) {
            auto deqCb = std::move(localDeqCallbacks.front());
            localDeqCallbacks.pop();
            try { deqCb(std::nullopt); } catch (...) {}
        }

        while (!localEnqCallbacks.empty()) {
            auto enqCb = std::move(localEnqCallbacks.front());
            localEnqCallbacks.pop();
            try { enqCb(true/*closed*/); } catch (...) {}
        }
    }

    ~AsyncMPMCQueue() {
        Close();
    }

private:
    std::mutex mutex_;
    bool isClosed_{false};
    std::queue<EnqueueCallback> enqCbQueue_;

    std::queue<T> valuesQueue_;
    std::queue<DequeueCallback> deqCbQueue_;
};

} // namespace bounded