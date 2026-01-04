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

package io.github.oasis.core.services.api.services;

import io.github.oasis.core.Game;
import io.github.oasis.core.elements.ElementDef;
import io.github.oasis.core.services.api.TestUtils;
import io.github.oasis.core.services.api.to.ElementCreateRequest;
import io.github.oasis.core.services.api.to.ElementUpdateRequest;
import io.github.oasis.core.services.api.to.GameCreateRequest;
import io.github.oasis.core.services.events.RuleChangeEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for rule change event publishing.
 * Verifies that ElementService correctly publishes RuleChangeEvents
 * when elements are added, updated, or deleted.
 *
 * @author Isuru Weerarathna
 */
public class RuleChangeEventTest extends AbstractServiceTest {

    @Autowired
    private IElementService elementService;

    @Autowired
    private ApplicationEventPublisher eventPublisher;

    private ApplicationEventPublisher spyPublisher;

    private int gameId;
    private ElementCreateRequest samplePoint;
    private ElementCreateRequest sampleBadge;

    private final GameCreateRequest testGame = GameCreateRequest.builder()
            .name("Test-Game-For-Rules")
            .description("Test game for rule change events")
            .logoRef("https://oasis.io/assets/test.jpeg")
            .motto("Testing rules")
            .build();

    @BeforeEach
    void beforeEachTest() {
        // Create a game first
        gameId = doPostSuccess("/games", testGame, Game.class).getId();

        // Load sample element definitions
        List<ElementCreateRequest> elementCreateRequests = TestUtils.parseElementRules("rules.yml", gameId);
        samplePoint = TestUtils.findById("testpoint", elementCreateRequests);
        sampleBadge = TestUtils.findById("testbadge", elementCreateRequests);

        // Create a spy of the event publisher and inject it
        spyPublisher = Mockito.spy(eventPublisher);
        ReflectionTestUtils.setField(elementService, "eventPublisher", spyPublisher);
    }

    @Test
    void addElement_ShouldPublishAddEvent() {
        // Reset spy to clear any previous interactions
        Mockito.reset(spyPublisher);

        // Add element
        ElementDef addedElement = doPostSuccess("/games/" + gameId + "/elements", samplePoint, ElementDef.class);

        // Verify event was published
        ArgumentCaptor<RuleChangeEvent> eventCaptor = ArgumentCaptor.forClass(RuleChangeEvent.class);
        Mockito.verify(spyPublisher, Mockito.atLeastOnce()).publishEvent(eventCaptor.capture());

        // Find the RuleChangeEvent in captured events
        RuleChangeEvent capturedEvent = findRuleChangeEvent(eventCaptor.getAllValues());
        assertNotNull(capturedEvent, "RuleChangeEvent should be published");
        assertEquals(RuleChangeEvent.ChangeType.ADD, capturedEvent.getChangeType());
        assertEquals(gameId, capturedEvent.getGameId());
        assertEquals(addedElement.getElementId(), capturedEvent.getElementDef().getElementId());
    }

    @Test
    void updateElement_ShouldPublishUpdateEvent() {
        // First add an element
        ElementDef addedElement = doPostSuccess("/games/" + gameId + "/elements", samplePoint, ElementDef.class);

        // Reset spy to only capture update events
        Mockito.reset(spyPublisher);

        // Update the element
        ElementUpdateRequest updateRequest = ElementUpdateRequest.builder()
                .name("Updated Point Name")
                .description("Updated description")
                .build();

        doPatchSuccess("/games/" + gameId + "/elements/" + addedElement.getElementId(), updateRequest, ElementDef.class);

        // Verify event was published
        ArgumentCaptor<RuleChangeEvent> eventCaptor = ArgumentCaptor.forClass(RuleChangeEvent.class);
        Mockito.verify(spyPublisher, Mockito.atLeastOnce()).publishEvent(eventCaptor.capture());

        RuleChangeEvent capturedEvent = findRuleChangeEvent(eventCaptor.getAllValues());
        assertNotNull(capturedEvent, "RuleChangeEvent should be published");
        assertEquals(RuleChangeEvent.ChangeType.UPDATE, capturedEvent.getChangeType());
        assertEquals(gameId, capturedEvent.getGameId());
        assertEquals(addedElement.getElementId(), capturedEvent.getElementDef().getElementId());
    }

    @Test
    void deleteElement_ShouldPublishRemoveEvent() {
        // First add an element
        ElementDef addedElement = doPostSuccess("/games/" + gameId + "/elements", samplePoint, ElementDef.class);

        // Reset spy to only capture delete events
        Mockito.reset(spyPublisher);

        // Delete the element
        doDeleteSuccess("/games/" + gameId + "/elements/" + addedElement.getElementId(), ElementDef.class);

        // Verify event was published
        ArgumentCaptor<RuleChangeEvent> eventCaptor = ArgumentCaptor.forClass(RuleChangeEvent.class);
        Mockito.verify(spyPublisher, Mockito.atLeastOnce()).publishEvent(eventCaptor.capture());

        RuleChangeEvent capturedEvent = findRuleChangeEvent(eventCaptor.getAllValues());
        assertNotNull(capturedEvent, "RuleChangeEvent should be published");
        assertEquals(RuleChangeEvent.ChangeType.REMOVE, capturedEvent.getChangeType());
        assertEquals(gameId, capturedEvent.getGameId());
        assertEquals(addedElement.getElementId(), capturedEvent.getElementDef().getElementId());
    }

    @Test
    void addMultipleElements_ShouldPublishMultipleAddEvents() {
        Mockito.reset(spyPublisher);

        // Add multiple elements
        doPostSuccess("/games/" + gameId + "/elements", samplePoint, ElementDef.class);
        doPostSuccess("/games/" + gameId + "/elements", sampleBadge, ElementDef.class);

        // Verify two events were published
        ArgumentCaptor<RuleChangeEvent> eventCaptor = ArgumentCaptor.forClass(RuleChangeEvent.class);
        Mockito.verify(spyPublisher, Mockito.atLeast(2)).publishEvent(eventCaptor.capture());

        long ruleChangeEventCount = eventCaptor.getAllValues().stream()
                .filter(e -> e instanceof RuleChangeEvent)
                .count();
        assertTrue(ruleChangeEventCount >= 2, "At least 2 RuleChangeEvents should be published");
    }

    /**
     * Helper method to find RuleChangeEvent from a list of captured events.
     * This is needed because Spring may publish other events as well.
     */
    private RuleChangeEvent findRuleChangeEvent(List<?> events) {
        return events.stream()
                .filter(e -> e instanceof RuleChangeEvent)
                .map(e -> (RuleChangeEvent) e)
                .findFirst()
                .orElse(null);
    }
}
