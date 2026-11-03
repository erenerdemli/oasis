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

package io.github.oasis.ext.kafkastream.runners;

import io.github.oasis.ext.kafkastream.GameEventHandler;
import io.github.oasis.ext.kafkastream.GameEventOffsets;
import io.github.oasis.ext.kafkastream.KafkaMessageId;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Reads the events of one game topic. Offsets are committed only once the engine has finished
 * the events, so that events an engine still holds when it stops, or when its partitions move to
 * another engine, are read again by the next owner instead of being lost.
 *
 * @author Isuru Weerarathna
 */
public class KafkaConsumerRunner implements Runnable, Closeable {

    /**
     * Events the engine may hold per game before reading pauses. Keeps the wait for unfinished
     * events short when partitions move or the engine stops.
     */
    private static final int MAX_IN_FLIGHT = 1000;
    private static final Duration FINISH_TIMEOUT = Duration.ofSeconds(30);

    protected KafkaConsumer<String, String> consumer;
    protected GameEventHandler handler;
    protected final String topic;
    protected volatile boolean running = true;
    private final GameEventOffsets offsets;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerRunner.class);

    public KafkaConsumerRunner(String forTheTopic) {
        this(forTheTopic, null);
    }

    public KafkaConsumerRunner(String forTheTopic, GameEventOffsets offsets) {
        this.topic = forTheTopic;
        this.offsets = offsets;
    }

    public void init(Properties properties, GameEventHandler eventHandler) {
        consumer = new KafkaConsumer<>(properties);
        this.handler = eventHandler;
    }

    /**
     * Called by the engine once it has finished an event, successfully or not.
     */
    public void finished(KafkaMessageId messageId) {
        offsets.finished(new TopicPartition(messageId.getTopic(), messageId.getPartition()), messageId.getOffset());
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(topic), new HandOver());

            while (running) {
                ConsumerRecords<String, String> polledRecords = consumer.poll(Duration.ofSeconds(1));

                if (!polledRecords.isEmpty()) {
                    handler.processEvents(polledRecords);
                }
                throttle();
                commit(offsets.toCommit());
            }
        } catch (WakeupException e) {
            // ignore
        } catch(Exception x){
            LOG.error("Unable to process Kafka Consumer Message ",x);
        } finally {
            finishAndCommit(consumer.assignment());
            consumer.close();
        }
    }

    private void throttle() {
        if (offsets.inFlight() >= MAX_IN_FLIGHT) {
            consumer.pause(consumer.assignment());
        } else if (!consumer.paused().isEmpty()) {
            consumer.resume(consumer.paused());
        }
    }

    private void commit(Map<TopicPartition, OffsetAndMetadata> commits) {
        if (commits.isEmpty()) {
            return;
        }
        consumer.commitSync(commits);
        offsets.committed(commits);
    }

    /**
     * Waits for the engine to finish the events of the given partitions and commits them. Events
     * not finished in time are read again by the next owner.
     */
    private void finishAndCommit(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            if (!offsets.awaitFinished(partitions, FINISH_TIMEOUT)) {
                LOG.warn("{} events of {} were not finished within {}. The next owner reads them again.",
                        offsets.inFlight(partitions), partitions, FINISH_TIMEOUT);
            }
            commit(offsets.toCommit(partitions));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error("Unable to commit finished events of {}!", partitions, e);
        }
    }

    private class HandOver implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            finishAndCommit(partitions);
            offsets.forget(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // reading starts from the committed offsets
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            // another engine owns them already, so they can no longer be committed
            offsets.forget(partitions);
        }
    }

    @Override
    public void close() {
        if (running) {
            running = false;
        }
        if (consumer != null) {
            consumer.wakeup();
        }
    }

}