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

import io.github.oasis.core.configs.OasisConfigs;
import io.github.oasis.core.exception.OasisRuntimeException;
import io.github.oasis.core.external.EngineManagerSubscription;
import io.github.oasis.core.external.messages.EngineStartedMessage;
import io.github.oasis.core.external.messages.EngineStatusChangedMessage;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * This class will create a kafka consumer from game engine status topic indicated by
 * {@link KafkaConstants#TOPIC_ENGINE_RELATED_EVENTS}.
 *
 * @author Isuru Weerarathna
 */
class KafkaEngineManagerSubscription implements EngineManagerSubscription {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaEngineManagerSubscription.class);

    private static final int ADMIN_TIMEOUT_SECONDS = 10;

    private final ExecutorService runner = Executors.newSingleThreadExecutor();

    private Properties consumerProps;
    private Admin admin;
    private KafkaConsumer<String, String> consumer;
    private Consumer<EngineStatusChangedMessage> engineMessageConsumer;
    private Consumer<EngineStartedMessage> engineStartedConsumer;
    private volatile boolean running = true;

    @Override
    public void init(OasisConfigs oasisConfigs) {
        Map<String, Object> configs = oasisConfigs.getObject("oasis.eventstream.configs");

        try {
            KafkaConfigs kafkaConfigs = KafkaUtils.parseFrom(configs);
            consumerProps = KafkaUtils.getEngineMgmtStreamConsumerProps(kafkaConfigs);
            admin = Admin.create(KafkaUtils.createAdminProps(kafkaConfigs));
        } catch (IOException e) {
            throw new OasisRuntimeException("Unable to initialize kafka engine events subscription due to config issue!", e);
        }
    }

    @Override
    public void subscribe(Consumer<EngineStatusChangedMessage> engineStatusChangedMessageConsumer,
                          Consumer<EngineStartedMessage> engineStartedMessageConsumer) {
        if (consumerProps == null || consumerProps.isEmpty()) {
            throw new OasisRuntimeException("Kafka engine subscription is not yet initialized! Did you call init method?");
        }

        engineMessageConsumer = engineStatusChangedMessageConsumer;
        engineStartedConsumer = engineStartedMessageConsumer;
        consumer = new KafkaConsumer<>(consumerProps);

        runner.submit(this::run);
        runner.shutdown();
    }

    /**
     * Engines read a game as members of the game's consumer group, so its members are the engines.
     */
    @Override
    public int countEnginesReading(int gameId) {
        String groupId = String.format(KafkaConstants.GAME_EVENT_CONSUMER_GROUP, gameId);
        try {
            ConsumerGroupDescription group = admin.describeConsumerGroups(Collections.singletonList(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return group.members().size();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OasisRuntimeException("Interrupted while reading the engines of game " + gameId + "!", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new OasisRuntimeException("Unable to read the engines of game " + gameId + "!", e);
        }
    }

    private void run() {
        try {
            consumer.subscribe(Collections.singletonList(KafkaConstants.TOPIC_ENGINE_RELATED_EVENTS));

            while (running) {
                ConsumerRecords<String, String> polledRecords = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> polledRecord : polledRecords) {
                    try {
                        if (isEngineStarted(polledRecord)) {
                            engineStartedConsumer.accept(MessageSerializer.deserialize(polledRecord.value(), EngineStartedMessage.class));
                        } else {
                            engineMessageConsumer.accept(MessageSerializer.deserialize(polledRecord.value(), EngineStatusChangedMessage.class));
                        }
                    } catch (Exception e) {
                        LOG.error("Error parsing or processing record {}", polledRecord);
                        LOG.error("Error: ", e);
                    }
                }

            }
        } catch (WakeupException e) {
            // ignore
        } finally {
            consumer.close();
        }
    }

    static boolean isEngineStarted(ConsumerRecord<String, String> record) {
        Header type = record.headers().lastHeader(KafkaConstants.HEADER_MESSAGE_TYPE);
        return type != null && KafkaConstants.ENGINE_STARTED.equals(new String(type.value(), StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        running = false;
        runner.shutdownNow();
        if (consumer != null) {
            consumer.close();
        }
        if (admin != null) {
            admin.close();
        }
    }
}
