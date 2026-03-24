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

#ifndef WORKER_LOCAL_REORDER_STAGE_H
#define WORKER_LOCAL_REORDER_STAGE_H

#include <algorithm>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <queue>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

/**
 * WorkerLocalReorderStage
 *
 * A worker-side in-memory reorder buffer for temporal ingestion.
 *
 * - Enabled mode: orders edges by event time with bounded lateness.
 * - Disabled mode: pass-through (drainReady() returns insertion order).
 */
class WorkerLocalReorderStage {
 public:
    using json = nlohmann::json;

    WorkerLocalReorderStage(bool enabled,
                            uint64_t allowedLatenessMs,
                            size_t maxBufferSize)
        : enabled_(enabled),
          allowedLatenessMs_(allowedLatenessMs),
          maxBufferSize_(std::max<size_t>(1, maxBufferSize)) {
    }

    void push(json edge, int64_t eventTimeMs, uint64_t sequenceNumber) {
        if (!enabled_) {
            passthroughQueue_.push_back(std::move(edge));
            return;
        }

        if (eventTimeMs > maxSeenEventTimeMs_) {
            maxSeenEventTimeMs_ = eventTimeMs;
        }

        reorderBuffer_.push(BufferedEdge{eventTimeMs,
                                         sequenceNumber,
                                         std::make_shared<json>(std::move(edge))});
    }

    std::vector<json> drainReady() {
        std::vector<json> out;

        while (!passthroughQueue_.empty()) {
            out.emplace_back(std::move(passthroughQueue_.front()));
            passthroughQueue_.pop_front();
        }

        if (!enabled_ || reorderBuffer_.empty()) {
            return out;
        }

        const int64_t watermark = maxSeenEventTimeMs_ - static_cast<int64_t>(allowedLatenessMs_);
        while (!reorderBuffer_.empty()) {
            const BufferedEdge& head = reorderBuffer_.top();
            if (head.eventTimeMs > watermark && reorderBuffer_.size() <= maxBufferSize_) {
                break;
            }

            BufferedEdge edge = reorderBuffer_.top();
            reorderBuffer_.pop();
            out.emplace_back(std::move(*edge.edgeJson));
        }

        return out;
    }

    std::vector<json> flushAll() {
        std::vector<json> out = drainReady();
        while (!reorderBuffer_.empty()) {
            BufferedEdge edge = reorderBuffer_.top();
            reorderBuffer_.pop();
            out.emplace_back(std::move(*edge.edgeJson));
        }
        return out;
    }

    static int64_t extractEventTimeMs(const json& edgeJson, int64_t fallbackValue) {
        static const std::vector<std::string> kCandidateKeys = {
            "event_time_ms", "eventTimeMs", "event_time", "eventTime", "timestamp_ms", "timestamp", "ts"};

        if (!edgeJson.contains("properties")) {
            return fallbackValue;
        }

        const auto& properties = edgeJson["properties"];
        if (!properties.is_object()) {
            return fallbackValue;
        }

        for (const auto& key : kCandidateKeys) {
            if (!properties.contains(key)) {
                continue;
            }

            const auto& value = properties[key];
            try {
                if (value.is_number_integer()) {
                    return value.get<int64_t>();
                }
                if (value.is_number_float()) {
                    return static_cast<int64_t>(value.get<double>());
                }
                if (value.is_string()) {
                    const std::string text = value.get<std::string>();
                    if (!text.empty()) {
                        return std::stoll(text);
                    }
                }
            } catch (...) {
                continue;
            }
        }

        return fallbackValue;
    }

 private:
    struct BufferedEdge {
        int64_t eventTimeMs;
        uint64_t sequenceNumber;
        std::shared_ptr<json> edgeJson;
    };

    struct BufferedEdgeOrder {
        bool operator()(const BufferedEdge& lhs, const BufferedEdge& rhs) const {
            if (lhs.eventTimeMs != rhs.eventTimeMs) {
                return lhs.eventTimeMs > rhs.eventTimeMs;
            }
            return lhs.sequenceNumber > rhs.sequenceNumber;
        }
    };

    bool enabled_;
    uint64_t allowedLatenessMs_;
    size_t maxBufferSize_;
    int64_t maxSeenEventTimeMs_ = std::numeric_limits<int64_t>::min();

    std::priority_queue<BufferedEdge, std::vector<BufferedEdge>, BufferedEdgeOrder> reorderBuffer_;
    std::deque<json> passthroughQueue_;
};

#endif  // WORKER_LOCAL_REORDER_STAGE_H
