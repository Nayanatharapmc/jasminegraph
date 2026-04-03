/**
Copyright 2026 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#ifndef LATE_EDGE_CORRECTION_QUEUE_H
#define LATE_EDGE_CORRECTION_QUEUE_H

#include <cstdint>
#include <deque>
#include <vector>

#include <nlohmann/json.hpp>

/**
 * LateEdgeCorrectionQueue
 *
 * Keeps very-late edges off the hot ingestion path and applies them through
 * bounded correction batches.
 */
class LateEdgeCorrectionQueue {
 public:
    using json = nlohmann::json;

    struct CorrectionEdge {
        json edge;
        int64_t eventTimeMs;
        uint32_t targetSnapshotId;
    };

    void enqueue(json edge, int64_t eventTimeMs, uint32_t targetSnapshotId) {
        queue_.push_back(CorrectionEdge{std::move(edge), eventTimeMs, targetSnapshotId});
    }

    std::vector<CorrectionEdge> drainBatch(size_t maxBatchSize) {
        std::vector<CorrectionEdge> out;
        if (maxBatchSize == 0) {
            return out;
        }

        const size_t count = std::min(maxBatchSize, queue_.size());
        out.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            out.emplace_back(std::move(queue_.front()));
            queue_.pop_front();
        }
        return out;
    }

    std::vector<CorrectionEdge> drainAll() {
        std::vector<CorrectionEdge> out;
        out.reserve(queue_.size());

        while (!queue_.empty()) {
            out.emplace_back(std::move(queue_.front()));
            queue_.pop_front();
        }

        return out;
    }

    size_t size() const {
        return queue_.size();
    }

 private:
    std::deque<CorrectionEdge> queue_;
};

#endif  // LATE_EDGE_CORRECTION_QUEUE_H
