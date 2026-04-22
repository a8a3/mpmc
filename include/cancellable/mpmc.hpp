#pragma once

#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <queue>
#include <unordered_map>

namespace cancellable {

template <typename T>
class AsyncMPMCQueue {
public:
    using CallbackId = std::uint64_t;

    AsyncMPMCQueue() = default;

    bool Enqueue(T value) {
        std::unique_lock lock{mutex_};
        if (isClosed_) return false;

        if (!cbList_.empty()) {
            auto [execCb, execCbId] = std::move(cbList_.front());
            cbList_.pop_front();
            cbMap_.erase(execCbId);
            lock.unlock();

            try { execCb(std::move(value)); } catch (...) {}
        } else {
            valuesQueue_.push(std::move(value));
        }
        return true;
    }

    using DequeueCallback = std::function<void(std::optional<T>)>;
    std::optional<CallbackId> Dequeue(DequeueCallback cb) {
        std::unique_lock lock{mutex_};
        if (!valuesQueue_.empty() || isClosed_) {
            std::optional<T> val = std::nullopt;
            if (!valuesQueue_.empty()) {
                val = std::move(valuesQueue_.front());
                valuesQueue_.pop();
            } 
            lock.unlock();

            try { cb(std::move(val)); } catch (...) {}
            return std::nullopt;
        } else {
            cbList_.push_back(std::make_pair(std::move(cb), cbId_));
            cbMap_.emplace(cbId_, std::prev(cbList_.cend()));
        } 
        return cbId_++;
    }
    
    bool Cancel(CallbackId id) {
        std::lock_guard guard{mutex_};
        if (isClosed_) return false;

        auto iter = cbMap_.find(id);
        if (std::end(cbMap_) == iter) return false;

        cbList_.erase(iter->second);
        cbMap_.erase(id);
        return true;
    }

    void Close() {
        std::unique_lock lock{mutex_};
        if (isClosed_) return;

        decltype(cbList_) localCallbacks;
        cbList_.swap(localCallbacks);

        decltype(cbMap_) localCbMap;
        cbMap_.swap(localCbMap);

        isClosed_ = true;
        lock.unlock();

        while (!localCallbacks.empty()) {
            auto [execCb, _] = std::move(localCallbacks.front());
            localCallbacks.pop_front();
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

    CallbackId cbId_{0};
    std::list<std::pair<DequeueCallback, CallbackId>> cbList_;
    using CallbackIter = typename decltype(cbList_)::const_iterator;
    std::unordered_map<CallbackId, CallbackIter> cbMap_;


    // 1. execution. 
    //    + dequeue callback; -> front, pop
    //    + remove mapping;   -> remove by id;
    //    + execute callback;

    // 2. cancellation
    //    + id lookup -> CallbackIter;
    //    + remove from the list by iter;
    //    + remove from the map by id;
};

} // namespace cancellable