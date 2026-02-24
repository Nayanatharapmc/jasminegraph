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

#include "FlushManager.h"

std::unordered_map<std::fstream*, std::shared_ptr<FlushManager::StreamState>> FlushManager::streamStates;
std::mutex FlushManager::registryMutex;

std::shared_ptr<FlushManager::StreamState> FlushManager::getStreamState(std::fstream* stream) {
    // Fast path: check if already exists (read-only, no lock needed for find)
    {
        std::lock_guard<std::mutex> lock(registryMutex);
        auto it = streamStates.find(stream);
        if (it != streamStates.end()) {
            return it->second;
        }
    }
    
    // Slow path: create new state
    auto newState = std::make_shared<StreamState>();
    {
        std::lock_guard<std::mutex> lock(registryMutex);
        // Double-check in case another thread created it
        auto it = streamStates.find(stream);
        if (it != streamStates.end()) {
            return it->second;
        }
        streamStates[stream] = newState;
        return newState;
    }
}

void FlushManager::recordWrite(std::fstream* stream) {
    if (!stream || !stream->is_open()) {
        return;
    }
    
    // Get stream state (only locks registry on first access per stream)
    auto state = getStreamState(stream);
    
    // Lock-free atomic increment 
    unsigned int count = state->writeCount.fetch_add(1, std::memory_order_relaxed) + 1;
    
    // Check if flush is needed (lock-free check)
    if (count % FLUSH_BATCH_SIZE == 0) {
        // Only lock during actual flush operation
        std::lock_guard<std::mutex> lock(state->flushMutex);
        stream->flush();
    }
}

void FlushManager::forceFlush(std::fstream* stream) {
    if (!stream || !stream->is_open()) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(registryMutex);
    auto it = streamStates.find(stream);
    if (it != streamStates.end()) {
        std::lock_guard<std::mutex> flushLock(it->second->flushMutex);
        stream->flush();
    } else {
        // Stream not registered, just flush
        stream->flush();
    }
}

void FlushManager::unregisterStream(std::fstream* stream) {
    std::lock_guard<std::mutex> lock(registryMutex);
    streamStates.erase(stream);
}
