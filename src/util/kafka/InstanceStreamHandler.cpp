/*
 * Copyright 2023 JasminGraph Team
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "InstanceStreamHandler.h"
#include "../../localstore/incremental/JasmineGraphIncrementalLocalStore.h"
#include "../Utils.h"
#include "../logger/Logger.h"

Logger instance_stream_logger;
InstanceStreamHandler::InstanceStreamHandler(std::map<std::string,
                                             JasmineGraphIncrementalLocalStore*>& incrementalLocalStoreMap)
        : incrementalLocalStoreMap(incrementalLocalStoreMap) { }

InstanceStreamHandler::~InstanceStreamHandler() { }

void InstanceStreamHandler::handleRequest(const std::string& nodeString) {
    if (nodeString == "-1") {
        instance_stream_logger.info("Received end-of-stream marker. Terminating stream threads.");
        terminateThreads = true;

        for (auto& cv : cond_vars) {
            cv.second.notify_all();
        }

         for (auto& thread : threads) {
             if (thread.second.joinable()) {
                 thread.second.join();
             }
         }

        return;
    }
    std::string graphIdentifier = extractGraphIdentifier(nodeString);
    if (graphIdentifier.empty()) {
        instance_stream_logger.error("Unable to extract graph identifier from payload. Dropping message.");
        return;
    }

    std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);  // Use specific mutex for the queue
    if (threads.find(graphIdentifier) == threads.end()) {
        queues[graphIdentifier] = std::queue<std::string>();
        threads[graphIdentifier] = std::thread(&InstanceStreamHandler::threadFunction, this, nodeString);
        instance_stream_logger.info("Started stream thread for " + graphIdentifier);
    }

    queues[graphIdentifier].push(nodeString);
    cond_vars[graphIdentifier].notify_one();
    instance_stream_logger.info("Queued message for " + graphIdentifier +
                                " (queue size=" + std::to_string(queues[graphIdentifier].size()) + ")");
}

void InstanceStreamHandler::threadFunction(const std::string& nodeString) {
    std::string graphIdentifier = extractGraphIdentifier(nodeString);
    if (graphIdentifier.empty()) {
        instance_stream_logger.error("Thread started with empty graph identifier. Exiting thread.");
        return;
    }
    if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
        auto graphIdPartitionId = JasmineGraphIncrementalLocalStore::getIDs(nodeString);
        std::string graphId = graphIdPartitionId.first;
        std::string partitionId = std::to_string(graphIdPartitionId.second);
        instance_stream_logger.info("Loading streaming store for graphId=" + graphId + " partitionId=" +
                                    partitionId);
        loadStreamingStore(graphId, partitionId, incrementalLocalStoreMap);
    }
    JasmineGraphIncrementalLocalStore* localStore = incrementalLocalStoreMap[graphIdentifier];
    instance_stream_logger.info("Stream worker thread running for " + graphIdentifier);

    while (!terminateThreads) {
        std::string nodeString;
        {
            std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);
            cond_vars[graphIdentifier].wait(lock, [&]{
                return !queues[graphIdentifier].empty() || terminateThreads;
            });

            if (terminateThreads) {
                break;
            }
            nodeString = queues[graphIdentifier].front();
            queues[graphIdentifier].pop();
        }
        instance_stream_logger.info("Processing message for " + graphIdentifier);
        localStore->addEdgeFromString(nodeString);
    }
    instance_stream_logger.info("Stream worker thread stopped for " + graphIdentifier);
}

std::string InstanceStreamHandler::extractGraphIdentifier(const std::string& nodeString) {
    auto graphIdPartitionId = JasmineGraphIncrementalLocalStore::getIDs(nodeString);
    std::string graphId = graphIdPartitionId.first;
    std::string partitionId = std::to_string(graphIdPartitionId.second);
    if (graphId.empty()) {
        instance_stream_logger.error("Missing graphId in streaming payload.");
        return "";
    }
    std::string graphIdentifier = graphId + "_" + partitionId;
    return graphIdentifier;
}

JasmineGraphIncrementalLocalStore *
InstanceStreamHandler::loadStreamingStore(std::string graphId, std::string partitionId, map<std::string,
                                          JasmineGraphIncrementalLocalStore *> &graphDBMapStreamingStores,
                                          std::string dbFilesOpenMode , bool isEmbed ) {
    std::string graphIdentifier = graphId + "_" + partitionId;
    instance_stream_logger.info("###INSTANCE### Loading streaming Store for" + graphIdentifier
                               + " : Started");
    std::string folderLocation = Utils::getJasmineGraphProperty("org.jasminegraph.server.instance.datafolder");
    instance_stream_logger.info("Streaming store data folder: " + folderLocation);
    auto *jasmineGraphStreamingLocalStore = new JasmineGraphIncrementalLocalStore(
                                     stoi(graphId), stoi(partitionId), dbFilesOpenMode, isEmbed);
    graphDBMapStreamingStores.insert(std::make_pair(graphIdentifier, jasmineGraphStreamingLocalStore));
    instance_stream_logger.info("###INSTANCE### Loading Local Store : Completed");
    return jasmineGraphStreamingLocalStore;
}

void InstanceStreamHandler::handleLocalEdge(std::string edge, std::string graphId,
                                            std::string partitionId, std::string graphIdentifier , bool isEmbed) {
    std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);
    if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
        loadStreamingStore(graphId, partitionId, incrementalLocalStoreMap, NodeManager::FILE_MODE, isEmbed);
        // append mode
    }
    JasmineGraphIncrementalLocalStore* localStore = incrementalLocalStoreMap[graphIdentifier];
    instance_stream_logger.info("Handling local edge for " + graphIdentifier);
    // Use addEdgeFromString() to enable temporal logging (see TEMPORAL_DATA_FLOW.md Phase 3.1)
    localStore->addEdgeFromString(edge);
}

void InstanceStreamHandler::handleCentralEdge(std::string edge, std::string graphId,
                                              std::string partitionId, std::string graphIdentifier, bool isEmbed) {
    std::unique_lock<std::mutex> lock(queue_mutexes[graphIdentifier]);
    if (incrementalLocalStoreMap.find(graphIdentifier) == incrementalLocalStoreMap.end()) {
        loadStreamingStore(graphId, partitionId, incrementalLocalStoreMap, NodeManager::FILE_MODE ,
            isEmbed);  // append mode
    }
    JasmineGraphIncrementalLocalStore* localStore = incrementalLocalStoreMap[graphIdentifier];
    instance_stream_logger.info("Handling central edge for " + graphIdentifier);
    // Use addEdgeFromString() to enable temporal logging (see TEMPORAL_DATA_FLOW.md Phase 3.1)
    localStore->addEdgeFromString(edge);
}
