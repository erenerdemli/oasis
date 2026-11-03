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

import io.github.oasis.core.external.MessageReceiver;
import io.github.oasis.core.external.messages.EngineMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Isuru Weerarathna
 */
public class GameEventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GameEventHandler.class);

    private final MessageReceiver sinkRef;
    private final GameEventOffsets offsets;

    public GameEventHandler(MessageReceiver sinkRef, GameEventOffsets offsets) {
        this.sinkRef = sinkRef;
        this.offsets = offsets;
    }

    /**
     * Hands the events to the engine. Each event carries its position as message id, and the engine
     * acknowledges it by that id once it has finished. An event that cannot be read or handed over
     * is skipped.
     */
    public void processEvents(ConsumerRecords<String, String> polledRecords) {
        for (ConsumerRecord<String, String> record : polledRecords) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            offsets.started(topicPartition, record.offset());
            try {
                EngineMessage message = MessageSerializer.deserialize(record.value(), EngineMessage.class);
                message.setMessageId(KafkaMessageId.builder()
                        .topic(record.topic())
                        .partition(record.partition())
                        .offset(record.offset())
                        .build());
                sinkRef.submit(message);
            } catch (Exception e) {
                LOG.error("Skipping game event at {}-{}@{} that cannot be handed to the engine!",
                        record.topic(), record.partition(), record.offset(), e);
                offsets.finished(topicPartition, record.offset());
            }
        }
    }

}
