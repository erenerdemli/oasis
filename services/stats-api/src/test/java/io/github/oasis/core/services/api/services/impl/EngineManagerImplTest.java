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
 */

package io.github.oasis.core.services.api.services.impl;

import io.github.oasis.core.Game;
import io.github.oasis.core.configs.OasisConfigs;
import io.github.oasis.core.elements.ElementDef;
import io.github.oasis.core.external.EngineManagerSubscription;
import io.github.oasis.core.external.EventDispatcher;
import io.github.oasis.core.external.messages.EngineMessage;
import io.github.oasis.core.external.messages.EngineStatusChangedMessage;
import io.github.oasis.core.external.messages.GameState;
import io.github.oasis.core.model.GameStatus;
import io.github.oasis.core.services.api.services.IGameService;
import io.github.oasis.core.services.events.RuleChangeEvent;
import io.github.oasis.core.services.exceptions.OasisApiException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public class EngineManagerImplTest {

    private static final int GAME_ID = 1;
    private static final Game TEST_GAME = Game.builder()
            .id(GAME_ID)
            .name("Game-Test")
            .description("Test Game description")
            .logoRef("https://images.oasis.io/games/" + GAME_ID)
            .motto("All the way")
            .active(true)
            .createdAt(System.currentTimeMillis())
            .updatedAt(System.currentTimeMillis()).build();

    private EngineManagerImpl engineManager;
    private ElementService elementService;
    private IGameService gameService;
    private EventDispatcher eventDispatcher;
    private MockedEngineSubscription subscription;


    @BeforeEach
    void beforeEach() {
        elementService = Mockito.mock(ElementService.class);
        gameService = Mockito.mock(IGameService.class);
        eventDispatcher = Mockito.mock(EventDispatcher.class);
        subscription = new MockedEngineSubscription();

        Mockito.when(gameService.readGame(Mockito.eq(GAME_ID))).thenReturn(TEST_GAME);
        engineManager = new EngineManagerImpl(elementService, gameService, eventDispatcher, subscription);
        engineManager.beforeInitialized();
    }

    @Test
    void changeGameStatus() {

    }

    @Test
    void testGameEventConsumeGameExists() throws OasisApiException {
        ArgumentCaptor<String> newGameStatusCapture = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> gameIdCapture = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Long> updatedAtCapture = ArgumentCaptor.forClass(Long.class);

        var msg = new EngineStatusChangedMessage(GAME_ID, GameState.PAUSED, "engine-abcd", 123L, null);
        subscription.invoke(msg);

        Mockito.verify(gameService, Mockito.times(1))
                .changeStatusOfGameWithoutPublishing(gameIdCapture.capture(), newGameStatusCapture.capture(), updatedAtCapture.capture());
        assertEquals(gameIdCapture.getValue(), GAME_ID);
        assertEquals(newGameStatusCapture.getValue(), "paused");
        assertEquals(updatedAtCapture.getValue(), 123L);
    }

    @Test
    void testGameEventConsumeGameNotExists() throws OasisApiException {
        var msg = new EngineStatusChangedMessage(9999, GameState.STOPPED, "engine-abcd", 234L, null);
        subscription.invoke(msg);

        Mockito.verify(gameService, Mockito.never())
                .changeStatusOfGameWithoutPublishing(Mockito.anyInt(), Mockito.anyString(), Mockito.anyLong());
    }

    @Test
    void initEngineStatusSubscription() {
        subscription = Mockito.mock(MockedEngineSubscription.class);
        engineManager = new EngineManagerImpl(elementService, gameService, eventDispatcher, subscription);
        engineManager.beforeInitialized();
        Mockito.verify(subscription, Mockito.times(1)).subscribe(Mockito.any());

        Mockito.clearInvocations(subscription);
        engineManager = new EngineManagerImpl(elementService, gameService, eventDispatcher, null);
        Mockito.verify(subscription, Mockito.never()).subscribe(Mockito.any());
    }

    @Test
    void close() {
        try {
            new EngineManagerImpl(elementService, gameService, eventDispatcher, null).close();
        } catch (IOException e) {
            Assertions.fail("Should not expected to fail!");
        }

        try {
            new EngineManagerImpl(elementService, gameService, eventDispatcher, subscription).close();
        } catch (IOException e) {
            Assertions.fail("Should not expected to fail!");
        }
    }

    // ============================================================
    // Tests for notifyRuleChange
    // ============================================================

    @Test
    void notifyRuleChange_GameRunning_ShouldBroadcastAddMessage() throws Exception {
        // Setup: Game is running (status = "started")
        GameStatus runningStatus = new GameStatus();
        runningStatus.setGameId(GAME_ID);
        runningStatus.setStatus("started");
        Mockito.when(gameService.getCurrentGameStatus(GAME_ID)).thenReturn(runningStatus);

        ElementDef elementDef = createTestElementDef();

        // Act
        engineManager.notifyRuleChange(RuleChangeEvent.ChangeType.ADD, GAME_ID, elementDef);

        // Verify: Should broadcast message to engine
        ArgumentCaptor<EngineMessage> messageCaptor = ArgumentCaptor.forClass(EngineMessage.class);
        Mockito.verify(eventDispatcher, Mockito.times(1)).broadcast(messageCaptor.capture());

        EngineMessage capturedMessage = messageCaptor.getValue();
        assertEquals(EngineMessage.GAME_RULE_ADDED, capturedMessage.getType());
        assertEquals(GAME_ID, capturedMessage.getScope().getGameId());
        assertEquals("core:point", capturedMessage.getImpl());
    }

    @Test
    void notifyRuleChange_GameRunning_ShouldBroadcastUpdateMessage() throws Exception {
        // Setup: Game is running
        GameStatus runningStatus = new GameStatus();
        runningStatus.setGameId(GAME_ID);
        runningStatus.setStatus("STARTED"); // Test case-insensitivity
        Mockito.when(gameService.getCurrentGameStatus(GAME_ID)).thenReturn(runningStatus);

        ElementDef elementDef = createTestElementDef();

        // Act
        engineManager.notifyRuleChange(RuleChangeEvent.ChangeType.UPDATE, GAME_ID, elementDef);

        // Verify
        ArgumentCaptor<EngineMessage> messageCaptor = ArgumentCaptor.forClass(EngineMessage.class);
        Mockito.verify(eventDispatcher, Mockito.times(1)).broadcast(messageCaptor.capture());

        EngineMessage capturedMessage = messageCaptor.getValue();
        assertEquals(EngineMessage.GAME_RULE_UPDATED, capturedMessage.getType());
    }

    @Test
    void notifyRuleChange_GameRunning_ShouldBroadcastRemoveMessage() throws Exception {
        // Setup: Game is running
        GameStatus runningStatus = new GameStatus();
        runningStatus.setGameId(GAME_ID);
        runningStatus.setStatus("started");
        Mockito.when(gameService.getCurrentGameStatus(GAME_ID)).thenReturn(runningStatus);

        ElementDef elementDef = createTestElementDef();

        // Act
        engineManager.notifyRuleChange(RuleChangeEvent.ChangeType.REMOVE, GAME_ID, elementDef);

        // Verify
        ArgumentCaptor<EngineMessage> messageCaptor = ArgumentCaptor.forClass(EngineMessage.class);
        Mockito.verify(eventDispatcher, Mockito.times(1)).broadcast(messageCaptor.capture());

        EngineMessage capturedMessage = messageCaptor.getValue();
        assertEquals(EngineMessage.GAME_RULE_REMOVED, capturedMessage.getType());
    }

    @Test
    void notifyRuleChange_GameNotRunning_ShouldNotBroadcast() throws Exception {
        // Setup: Game is stopped
        GameStatus stoppedStatus = new GameStatus();
        stoppedStatus.setGameId(GAME_ID);
        stoppedStatus.setStatus("stopped");
        Mockito.when(gameService.getCurrentGameStatus(GAME_ID)).thenReturn(stoppedStatus);

        ElementDef elementDef = createTestElementDef();

        // Act
        engineManager.notifyRuleChange(RuleChangeEvent.ChangeType.ADD, GAME_ID, elementDef);

        // Verify: Should NOT broadcast to engine
        Mockito.verify(eventDispatcher, Mockito.never()).broadcast(Mockito.any(EngineMessage.class));
    }

    @Test
    void notifyRuleChange_GamePaused_ShouldNotBroadcast() throws Exception {
        // Setup: Game is paused
        GameStatus pausedStatus = new GameStatus();
        pausedStatus.setGameId(GAME_ID);
        pausedStatus.setStatus("paused");
        Mockito.when(gameService.getCurrentGameStatus(GAME_ID)).thenReturn(pausedStatus);

        ElementDef elementDef = createTestElementDef();

        // Act
        engineManager.notifyRuleChange(RuleChangeEvent.ChangeType.UPDATE, GAME_ID, elementDef);

        // Verify: Should NOT broadcast to engine (game not running)
        Mockito.verify(eventDispatcher, Mockito.never()).broadcast(Mockito.any(EngineMessage.class));
    }

    @Test
    void notifyRuleChange_GameStatusNull_ShouldNotBroadcast() throws Exception {
        // Setup: No game status found
        Mockito.when(gameService.getCurrentGameStatus(GAME_ID)).thenReturn(null);

        ElementDef elementDef = createTestElementDef();

        // Act
        engineManager.notifyRuleChange(RuleChangeEvent.ChangeType.ADD, GAME_ID, elementDef);

        // Verify: Should NOT broadcast to engine
        Mockito.verify(eventDispatcher, Mockito.never()).broadcast(Mockito.any(EngineMessage.class));
    }

    @Test
    void notifyRuleChange_GameStatusCheckThrowsException_ShouldNotBroadcast() throws Exception {
        // Setup: Exception when checking game status
        Mockito.when(gameService.getCurrentGameStatus(GAME_ID))
                .thenThrow(new RuntimeException("Database error"));

        ElementDef elementDef = createTestElementDef();

        // Act - should not throw
        engineManager.notifyRuleChange(RuleChangeEvent.ChangeType.ADD, GAME_ID, elementDef);

        // Verify: Should NOT broadcast to engine (error getting status, assume not running)
        Mockito.verify(eventDispatcher, Mockito.never()).broadcast(Mockito.any(EngineMessage.class));
    }

    private ElementDef createTestElementDef() {
        return ElementDef.builder()
                .elementId("test.point.rule")
                .gameId(GAME_ID)
                .type("core:point")
                .data(Map.of("id", "test.point.rule", "name", "Test Point Rule"))
                .build();
    }

    private static class MockedEngineSubscription implements EngineManagerSubscription {

        private Consumer<EngineStatusChangedMessage> consumer;

        @Override
        public void init(OasisConfigs configs) {

        }

        @Override
        public void subscribe(Consumer<EngineStatusChangedMessage> engineStatusChangedMessageConsumer) {
            this.consumer = engineStatusChangedMessageConsumer;
        }

        void invoke(EngineStatusChangedMessage msg) {
            this.consumer.accept(msg);
        }

        @Override
        public void close() {

        }
    }
}