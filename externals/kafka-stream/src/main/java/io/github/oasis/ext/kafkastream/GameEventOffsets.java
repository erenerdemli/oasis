/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 *
 */

package io.github.oasis.ext.kafkastream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Game events of one game topic that were handed to the engine and are not finished yet, per
 * partition. A partition is committed only up to its oldest unfinished event, so an event the
 * engine still holds in memory is read again by whichever engine owns the partition next.
 *
 * Events are started by the consumer thread and finished by the engine's actors.
 */
public class GameEventOffsets {

    private static final long WAIT_STEP_MS = 20;

    private final Map<TopicPartition, PartitionOffsets> partitions = new ConcurrentHashMap<>();

    public void started(TopicPartition partition, long offset) {
        partitions.computeIfAbsent(partition, p -> new PartitionOffsets()).started(offset);
    }

    public void finished(TopicPartition partition, long offset) {
        PartitionOffsets offsets = partitions.get(partition);
        if (offsets != null) {
            offsets.pending.remove(offset);
        }
    }

    public int inFlight() {
        return inFlight(partitions.keySet());
    }

    public int inFlight(Collection<TopicPartition> ofPartitions) {
        int count = 0;
        for (TopicPartition partition : ofPartitions) {
            PartitionOffsets offsets = partitions.get(partition);
            if (offsets != null) {
                count += offsets.pending.size();
            }
        }
        return count;
    }

    /**
     * Waits until the given partitions have no unfinished events, or the timeout passes.
     *
     * @return true when every event finished in time
     */
    public boolean awaitFinished(Collection<TopicPartition> ofPartitions, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (inFlight(ofPartitions) > 0) {
            if (System.currentTimeMillis() >= deadline) {
                return false;
            }
            Thread.sleep(WAIT_STEP_MS);
        }
        return true;
    }

    /**
     * Commit positions of the given partitions that moved since they were last committed.
     */
    public Map<TopicPartition, OffsetAndMetadata> toCommit(Collection<TopicPartition> ofPartitions) {
        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
        for (TopicPartition partition : ofPartitions) {
            PartitionOffsets offsets = partitions.get(partition);
            if (offsets != null) {
                long position = offsets.position();
                if (position > offsets.committed) {
                    commits.put(partition, new OffsetAndMetadata(position));
                }
            }
        }
        return commits;
    }

    public Map<TopicPartition, OffsetAndMetadata> toCommit() {
        return toCommit(partitions.keySet());
    }

    public void committed(Map<TopicPartition, OffsetAndMetadata> commits) {
        commits.forEach((partition, position) -> {
            PartitionOffsets offsets = partitions.get(partition);
            if (offsets != null) {
                offsets.committed = position.offset();
            }
        });
    }

    public void forget(Collection<TopicPartition> ofPartitions) {
        ofPartitions.forEach(partitions::remove);
    }

    private static class PartitionOffsets {

        private final ConcurrentSkipListSet<Long> pending = new ConcurrentSkipListSet<>();

        // offset after the newest started event, written by the consumer thread only
        private volatile long next = -1;
        private long committed = -1;

        private void started(long offset) {
            pending.add(offset);
            next = offset + 1;
        }

        /**
         * The oldest unfinished event, or the one after the newest when all are finished.
         */
        private long position() {
            // offsets are never negative, so this is the oldest pending one, or null when none is
            Long oldest = pending.ceiling(0L);
            return oldest == null ? next : oldest;
        }
    }
}
