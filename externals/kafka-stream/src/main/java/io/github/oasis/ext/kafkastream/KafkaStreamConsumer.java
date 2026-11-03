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

import io.github.oasis.core.context.RuntimeContextSupport;
import io.github.oasis.core.external.MessageReceiver;
import io.github.oasis.core.external.SourceStreamProvider;
import io.github.oasis.core.external.messages.EngineStartedMessage;
import io.github.oasis.core.external.messages.EngineStatusChangedMessage;
import io.github.oasis.core.external.messages.FailedGameCommand;
import io.github.oasis.core.external.messages.GameCommand;
import io.github.oasis.core.external.messages.GameCommand.GameLifecycle;
import io.github.oasis.core.external.messages.GameState;
import io.github.oasis.core.utils.Utils;
import io.github.oasis.ext.kafkastream.runners.KafkaBroadcastConsumerRunner;
import io.github.oasis.ext.kafkastream.runners.KafkaConsumerRunner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Isuru Weerarathna
 */
class KafkaStreamConsumer implements SourceStreamProvider {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamConsumer.class);

    private MessageReceiver sinkRef;

    public KafkaPublisher publisher;

    /**
     * How long closing waits for the game readers to finish and commit their events.
     */
    private static final long CLOSE_TIMEOUT_SECONDS = 40;

    // keyed by topic, so that an acknowledged message id leads straight to its reader
    private final Map<String, KafkaConsumerRunner> gameEventsConsumers = new ConcurrentHashMap<>();
    private KafkaConsumerRunner gameBroadcastConsumer;

    private ExecutorService consumerPool;

    private String engineId;
    private KafkaConfigs providedKafkaConfigs;

    @Override
    public void init(RuntimeContextSupport context, MessageReceiver sinkRef) throws Exception {
        this.sinkRef = sinkRef;

        var allConfigs = context.getConfigs().getObject("oasis.eventstream.configs");

        engineId = context.id();
        KafkaConfigs kafkaConfigs = KafkaUtils.parseFrom(allConfigs);
        providedKafkaConfigs = kafkaConfigs;

        // initialize kafka publisher for notifications...
        publisher = new KafkaPublisher(providedKafkaConfigs, engineId);

        consumerPool = Executors.newCachedThreadPool();

        LOG.debug("Initializing kafka broadcast topic consumer...");
        Properties broadcastingProps = KafkaUtils.getBroadcastConsumerProps(kafkaConfigs, engineId);
        String instanceId = broadcastingProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        String uniqueId = engineId + "::" + instanceId;
        gameBroadcastConsumer = new KafkaBroadcastConsumerRunner(sinkRef, KafkaConstants.TOPIC_GAME_ANNOUNCEMENTS, uniqueId);
        gameBroadcastConsumer.init(broadcastingProps, null);

        LOG.info("Connecting to kafka broadcasting topic: {} with configs {}...", KafkaConstants.TOPIC_GAME_ANNOUNCEMENTS, broadcastingProps);
        consumerPool.submit(gameBroadcastConsumer);

        announceEngineStarted();
    }

    /**
     * Tells the admin api that this engine has started, so that it announces the running games
     * again and this engine picks them up. The engine's broadcast group keeps its position across
     * restarts, so the announcements reach it whenever they are sent.
     */
    private void announceEngineStarted() {
        EngineStartedMessage message = new EngineStartedMessage(engineId, System.currentTimeMillis());
        try {
            publisher.publishRecord(KafkaConstants.TOPIC_ENGINE_RELATED_EVENTS, engineId, KafkaConstants.ENGINE_STARTED, message);
            LOG.info("Engine {} announced its start.", engineId);
        } catch (IOException e) {
            LOG.error("Unable to announce the start of engine {}! Running games must be started again from the admin.", engineId, e);
        }
    }

    @Override
    public void handleGameCommand(GameCommand gameCommand) {
        if (gameCommand instanceof FailedGameCommand) {
            LOG.warn("Failed game command received! [{}]", gameCommand);
            silentPublish(createStatusChangedMessage(gameCommand));
            return;
        }

        int gameId = gameCommand.getGameId();
        String topic = KafkaUtils.getGameEventTopicName(gameId);
        LOG.info("Processing game command. [Game: {}, Status = {}]", gameId, gameCommand.getStatus());
        if (gameCommand.getStatus() == GameLifecycle.START) {
            // a game is started again whenever an engine starts, so the readers of the engines
            // already running it keep their partitions instead of leaving and rejoining the group
            if (gameEventsConsumers.containsKey(topic)) {
                LOG.info("Already reading game {} events.", gameId);
            } else {
                GameEventOffsets offsets = new GameEventOffsets();
                KafkaConsumerRunner gameReader = new KafkaConsumerRunner(topic, offsets);
                try {
                    Properties thisConsumerProps = KafkaUtils.createGameEventConsumerProps(providedKafkaConfigs, gameId, engineId);
                    LOG.info("Subscribing to game {} event topic with configs {}...", gameId, thisConsumerProps);
                    gameReader.init(thisConsumerProps, new GameEventHandler(sinkRef, offsets));
                    gameEventsConsumers.put(topic, gameReader);
                    consumerPool.submit(gameReader);
                } catch (Exception e) {
                    LOG.error("Error initializing Kafka consumer for game {}!", gameId, e);
                    gameEventsConsumers.remove(topic);
                    Utils.silentClose(gameReader, (ex) -> LOG.error("Unable to close connection to kafka game event topic!", ex));

                    // we send game stopped message, if failed.
                    EngineStatusChangedMessage message = createStatusChangedMessage(gameCommand);
                    message.setState(GameState.STOPPED);
                    silentPublish(message);
                    return;
                }
            }

        } else if (gameCommand.getStatus() == GameLifecycle.REMOVE) {
            Closeable removedRef = gameEventsConsumers.remove(topic);
            if (Objects.nonNull(removedRef)) {
                LOG.info("Game consumer {} disconnected!", gameId);
                Utils.silentClose(removedRef, (e) -> LOG.error("Unable to close connection to kafka game event topic!", e));
            }

        }

        // publish game state only for successful handling.
        // this line should not be invoked for errors. They must be handled separately and returned immediately.
        silentPublish(createStatusChangedMessage(gameCommand));
    }

    private EngineStatusChangedMessage createStatusChangedMessage(GameCommand gameCommand) {
        EngineStatusChangedMessage message = new EngineStatusChangedMessage();
        message.setGameId(gameCommand.getGameId());
        message.setEngineId(engineId);
        message.setState(GameLifecycle.convertTo(gameCommand.getStatus()));
        message.setTs(System.currentTimeMillis());
        message.setTagData(gameCommand);
        return message;
    }

    private void silentPublish(Object payload) {
        try {
            publisher.publishRecord(KafkaConstants.TOPIC_ENGINE_RELATED_EVENTS, engineId, payload);
        } catch (IOException e) {
            LOG.error("Unable to publish engine event to topic {}!", KafkaConstants.TOPIC_ENGINE_RELATED_EVENTS);
            LOG.error("Error caused:", e);
        }
    }

    @Override
    public void ackMessage(int gameId, Object messageId) {
        finished(messageId);
    }

    @Override
    public void ackMessage(Object messageId) {
        finished(messageId);
    }

    /**
     * A failed event is not read again, as before: it is logged by the engine and skipped.
     */
    @Override
    public void nackMessage(int gameId, Object messageId) {
        finished(messageId);
    }

    @Override
    public void nackMessage(Object messageId) {
        finished(messageId);
    }

    /**
     * Marks a game event finished, so that its reader may commit past it. Broadcast messages are
     * committed one by one as they are read, so their ids find no reader here.
     */
    private void finished(Object messageId) {
        if (messageId instanceof KafkaMessageId) {
            KafkaMessageId id = (KafkaMessageId) messageId;
            KafkaConsumerRunner reader = gameEventsConsumers.get(id.getTopic());
            if (reader != null) {
                reader.finished(id);
            }
        }
    }

    /**
     * Stops reading and waits until the game readers have committed what the engine finished. The
     * engine must keep running meanwhile, see OasisEngine.
     */
    @Override
    public void close() {
        for (KafkaConsumerRunner reader : gameEventsConsumers.values()) {
            reader.close();
        }

        if (gameBroadcastConsumer != null) {
            gameBroadcastConsumer.close();
        }
        // gracefully shutdown
        consumerPool.shutdown();
        try {
            if (!consumerPool.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOG.warn("Game readers did not stop within {} seconds.", CLOSE_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


}
