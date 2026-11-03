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

import io.github.oasis.core.external.EventDispatcher;
import io.github.oasis.core.external.messages.EngineMessage;
import io.github.oasis.core.utils.Utils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Event dispatcher for Kafka.
 *
 * @author Isuru Weerarathna
 */
class KafkaDispatcher extends KafkaPublisher implements EventDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDispatcher.class);

    private Admin kafkaAdmin;
    private int gameEventPartitions = KafkaConstants.DEFAULT_GAME_EVENT_PARTITIONS;

    @Override
    public void init(DispatcherContext context) throws Exception {
        Map<String, Object> configs = context.getConfigs();
        KafkaConfigs kafkaConfigs = KafkaUtils.parseFrom(configs);
        if (kafkaConfigs.getDispatcherConfigs() != null && kafkaConfigs.getDispatcherConfigs().getGameEventPartitions() != null) {
            gameEventPartitions = kafkaConfigs.getDispatcherConfigs().getGameEventPartitions();
        }

        kafkaAdmin = Admin.create(KafkaUtils.createAdminProps(kafkaConfigs));
        createTopicsIfNotExists(kafkaAdmin);

        // initialize producer...
        super.initialize(kafkaConfigs);
    }

    /**
     * Creates the game's event topic with the configured partitions, or adds partitions to a topic
     * that has fewer, so that the game can be shared by that many engines. A topic created by the
     * broker on first use has the broker's default partitions only.
     *
     * Adding partitions moves some players to another partition, so their events already waiting
     * may be read out of order once.
     */
    @Override
    public void prepareGameStream(int gameId) throws Exception {
        String topic = getTopicForGameEvents(String.valueOf(gameId));
        Optional<TopicDescription> existing = describeTopic(topic);
        if (existing.isEmpty()) {
            LOG.info("Creating game event topic {} with {} partitions.", topic, gameEventPartitions);
            createTopic(kafkaAdmin, new NewTopic(topic, Optional.of(gameEventPartitions), Optional.empty()));
        } else if (existing.get().partitions().size() < gameEventPartitions) {
            LOG.info("Increasing partitions of game event topic {} from {} to {}.",
                    topic, existing.get().partitions().size(), gameEventPartitions);
            kafkaAdmin.createPartitions(Collections.singletonMap(topic, NewPartitions.increaseTo(gameEventPartitions)))
                    .all()
                    .get();
        }
    }

    private Optional<TopicDescription> describeTopic(String topic) throws Exception {
        try {
            return Optional.of(kafkaAdmin.describeTopics(Collections.singletonList(topic)).values().get(topic).get());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public void push(EngineMessage message) throws Exception {
        String gameId = String.valueOf(message.getScope().getGameId());
        String userId = String.valueOf(Utils.firstNonNull(message.getScope().getUserId(), -1));

        ProducerRecord<String, String> record = new ProducerRecord<>(
                getTopicForGameEvents(gameId),
                userId,
                MessageSerializer.serialize(message));

        kafkaProducer.send(record);
    }

    @Override
    public void broadcast(EngineMessage message) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                KafkaConstants.TOPIC_GAME_ANNOUNCEMENTS,
                MessageSerializer.serialize(message));

        kafkaProducer.send(record);
    }

    @Override
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close(Duration.ofSeconds(5));
        }
        if (kafkaAdmin != null) {
            kafkaAdmin.close(Duration.ofSeconds(5));
        }
    }

    private String getTopicForGameEvents(String gameId) {
        return KafkaConstants.TOPIC_GAME_EVENTS + KafkaConstants.TOPIC_DELIMITER + gameId;
    }

    private void createTopicsIfNotExists(Admin kafkaAdmin) throws IOException {
        NewTopic topic = new NewTopic(KafkaConstants.TOPIC_GAME_ANNOUNCEMENTS, Optional.empty(), Optional.empty());

        createTopic(kafkaAdmin, topic);
    }

}
