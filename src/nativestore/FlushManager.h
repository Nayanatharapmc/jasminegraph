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
**/

#ifndef FLUSH_MANAGER_H
#define FLUSH_MANAGER_H

#include <fstream>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <memory>

/**
 * FlushManager batches disk writes and periodically flushes to reduce I/O overhead.
 * Lock-free design: uses per-stream atomic counters for scalability at billion-scale ingestion.
 * Mutex only used during actual flush operations, not on every write.
 */
class FlushManager {
private:
    struct StreamState {
        std::atomic<unsigned int> writeCount{0};
        std::mutex flushMutex;  // Per-stream flush lock
    };
    
    // Per-stream state with atomic counters
    static std::unordered_map<std::fstream*, std::shared_ptr<StreamState>> streamStates;
    static std::mutex registryMutex;  // Only for map modifications, not for writes
    static const unsigned int FLUSH_BATCH_SIZE = 1000;
    
    // Get or create stream state (thread-safe)
    static std::shared_ptr<StreamState> getStreamState(std::fstream* stream);

public:
    /**
     * Record a write to a stream (lock-free except on flush)
     * Uses atomic increment - no mutex on normal path
     */
    static void recordWrite(std::fstream* stream);
    
    /**
     * Force flush a stream (e.g., on shutdown)
     */
    static void forceFlush(std::fstream* stream);
    
    /**
     * Clean up state for a closed stream
     */
    static void unregisterStream(std::fstream* stream);
};

#endif
