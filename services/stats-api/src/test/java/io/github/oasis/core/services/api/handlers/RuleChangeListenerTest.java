/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.oasis.core.services.api.handlers;

import io.github.oasis.core.elements.ElementDef;
import io.github.oasis.core.services.api.exceptions.EngineManagerException;
import io.github.oasis.core.services.api.services.IEngineManager;
import io.github.oasis.core.services.events.RuleChangeEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link RuleChangeListener}.
 *
 * @author Isuru Weerarathna
 */
class RuleChangeListenerTest {

    private static final int GAME_ID = 1;
    private static final String ELEMENT_ID = "test.point.rule";

    private IEngineManager engineManager;
    private RuleChangeListener listener;
    private ElementDef testElement;

    @BeforeEach
    void setUp() {
        engineManager = Mockito.mock(IEngineManager.class);
        listener = new RuleChangeListener(engineManager);

        testElement = ElementDef.builder()
                .elementId(ELEMENT_ID)
                .gameId(GAME_ID)
                .type("core:point")
                .data(Map.of("id", ELEMENT_ID, "name", "Test Point"))
                .build();
    }

    @Test
    void handleRuleChangeEvent_Add() throws EngineManagerException {
        RuleChangeEvent event = new RuleChangeEvent(
                RuleChangeEvent.ChangeType.ADD,
                GAME_ID,
                testElement
        );

        listener.handleRuleChangeEvent(event);

        verify(engineManager, times(1))
                .notifyRuleChange(eq(RuleChangeEvent.ChangeType.ADD), eq(GAME_ID), eq(testElement));
    }

    @Test
    void handleRuleChangeEvent_Update() throws EngineManagerException {
        RuleChangeEvent event = new RuleChangeEvent(
                RuleChangeEvent.ChangeType.UPDATE,
                GAME_ID,
                testElement
        );

        listener.handleRuleChangeEvent(event);

        verify(engineManager, times(1))
                .notifyRuleChange(eq(RuleChangeEvent.ChangeType.UPDATE), eq(GAME_ID), eq(testElement));
    }

    @Test
    void handleRuleChangeEvent_Remove() throws EngineManagerException {
        RuleChangeEvent event = new RuleChangeEvent(
                RuleChangeEvent.ChangeType.REMOVE,
                GAME_ID,
                testElement
        );

        listener.handleRuleChangeEvent(event);

        verify(engineManager, times(1))
                .notifyRuleChange(eq(RuleChangeEvent.ChangeType.REMOVE), eq(GAME_ID), eq(testElement));
    }

    @Test
    void handleRuleChangeEvent_EngineManagerThrowsException_ShouldNotPropagate() throws EngineManagerException {
        // When engine manager throws an exception, the listener should catch it
        // and not propagate it (since the DB change is already committed)
        doThrow(new EngineManagerException("TEST_ERROR", new RuntimeException("Test")))
                .when(engineManager)
                .notifyRuleChange(any(), anyInt(), any());

        RuleChangeEvent event = new RuleChangeEvent(
                RuleChangeEvent.ChangeType.ADD,
                GAME_ID,
                testElement
        );

        // Should not throw - the exception is caught and logged
        listener.handleRuleChangeEvent(event);

        verify(engineManager, times(1))
                .notifyRuleChange(eq(RuleChangeEvent.ChangeType.ADD), eq(GAME_ID), eq(testElement));
    }
}
