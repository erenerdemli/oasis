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
import io.github.oasis.core.elements.ElementStatus;
import io.github.oasis.core.services.api.TestUtils;
import io.github.oasis.core.services.api.exceptions.ErrorCodes;
import io.github.oasis.core.services.api.to.ElementCreateRequest;
import io.github.oasis.core.services.api.to.ElementUpdateRequest;
import io.github.oasis.core.services.api.to.GameCreateRequest;
import io.github.oasis.core.services.events.RuleChangeEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                .version(addedElement.getVersion())
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

    @Test
    void addPassiveElement_ShouldSaveItWithoutPublishing() {
        Mockito.reset(spyPublisher);

        Map<String, Object> designer = Map.of("form", "points", "amount", 15);
        ElementDef added = doPostSuccess("/games/" + gameId + "/elements",
                samplePoint.toBuilder().status(ElementStatus.PASSIVE).designer(designer).build(),
                ElementDef.class);

        assertEquals(ElementStatus.PASSIVE, added.getStatus());
        assertTrue(ruleChangeEvents().isEmpty(), "A passive rule must not reach the engines");

        ElementDef read = doGetSuccess("/games/" + gameId + "/elements/" + added.getElementId() + "?withData=true", ElementDef.class);
        assertEquals(ElementStatus.PASSIVE, read.getStatus());
        assertEquals(designer, read.getDesigner());
    }

    @Test
    void addElementWithoutStatus_ShouldBeActive() {
        ElementDef added = doPostSuccess("/games/" + gameId + "/elements", samplePoint, ElementDef.class);

        assertEquals(ElementStatus.ACTIVE, added.getStatus());
    }

    @Test
    void changeStatus_ShouldPublishRemoveThenAdd() {
        ElementDef added = doPostSuccess("/games/" + gameId + "/elements", samplePoint, ElementDef.class);
        String statusPath = "/games/" + gameId + "/elements/" + added.getElementId() + "/status/";

        Mockito.reset(spyPublisher);
        ElementDef passive = doPutSuccess(statusPath + "PASSIVE", null, ElementDef.class);
        assertEquals(ElementStatus.PASSIVE, passive.getStatus());
        assertChangeTypes(RuleChangeEvent.ChangeType.REMOVE);
        assertEquals(added.getData(), lastRuleChangeEvent().getElementDef().getData());

        Mockito.reset(spyPublisher);
        doPutSuccess(statusPath + "PASSIVE", null, ElementDef.class);
        assertChangeTypes();

        Mockito.reset(spyPublisher);
        ElementDef active = doPutSuccess(statusPath + "ACTIVE", null, ElementDef.class);
        assertEquals(ElementStatus.ACTIVE, active.getStatus());
        assertChangeTypes(RuleChangeEvent.ChangeType.ADD);
    }

    @Test
    void updateElementWithData_ShouldReplaceRuleAndPublishIt() {
        ElementDef added = doPostSuccess("/games/" + gameId + "/elements", samplePoint, ElementDef.class);
        Map<String, Object> newData = withAmount(samplePoint.getData(), 25);
        Map<String, Object> designer = Map.of("form", "points", "amount", 25);

        Mockito.reset(spyPublisher);
        doPatchSuccess("/games/" + gameId + "/elements/" + added.getElementId(), ElementUpdateRequest.builder()
                .name("Answer-Accepted")
                .version(added.getVersion())
                .data(newData)
                .designer(designer)
                .build(), ElementDef.class);

        assertChangeTypes(RuleChangeEvent.ChangeType.UPDATE);
        assertEquals(newData, lastRuleChangeEvent().getElementDef().getData());

        ElementDef read = doGetSuccess("/games/" + gameId + "/elements/" + added.getElementId() + "?withData=true", ElementDef.class);
        assertEquals(newData, read.getData());
        assertEquals(designer, read.getDesigner());
    }

    @Test
    void updateElementWithInvalidData_ShouldKeepTheRule() {
        ElementDef added = doPostSuccess("/games/" + gameId + "/elements", samplePoint, ElementDef.class);

        doPatchError("/games/" + gameId + "/elements/" + added.getElementId(), ElementUpdateRequest.builder()
                .name("Answer-Accepted")
                .version(added.getVersion())
                .data(Map.of("id", "testpoint", "name", "Answer-Accepted", "spec", Map.of("reward", Map.of("amount", 25))))
                .build(), HttpStatus.BAD_REQUEST, ErrorCodes.ELEMENT_SPEC_INVALID);

        ElementDef read = doGetSuccess("/games/" + gameId + "/elements/" + added.getElementId() + "?withData=true", ElementDef.class);
        assertEquals(added.getData(), read.getData());
        assertEquals(added.getVersion(), read.getVersion());
    }

    @Test
    void updateAndDeletePassiveElement_ShouldNotPublish() {
        ElementDef added = doPostSuccess("/games/" + gameId + "/elements",
                samplePoint.toBuilder().status(ElementStatus.PASSIVE).build(), ElementDef.class);

        Mockito.reset(spyPublisher);
        doPatchSuccess("/games/" + gameId + "/elements/" + added.getElementId(), ElementUpdateRequest.builder()
                .name("Answer-Accepted")
                .version(added.getVersion())
                .data(withAmount(samplePoint.getData(), 30))
                .build(), ElementDef.class);
        doDeleteSuccess("/games/" + gameId + "/elements/" + added.getElementId(), ElementDef.class);

        assertChangeTypes();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> withAmount(Map<String, Object> data, int amount) {
        Map<String, Object> spec = new HashMap<>((Map<String, Object>) data.get("spec"));
        Map<String, Object> reward = new HashMap<>((Map<String, Object>) spec.get("reward"));
        reward.put("amount", amount);
        spec.put("reward", reward);
        Map<String, Object> copy = new HashMap<>(data);
        copy.put("spec", spec);
        return copy;
    }

    private List<RuleChangeEvent> ruleChangeEvents() {
        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(spyPublisher, Mockito.atLeast(0)).publishEvent(eventCaptor.capture());
        return eventCaptor.getAllValues().stream()
                .filter(e -> e instanceof RuleChangeEvent)
                .map(e -> (RuleChangeEvent) e)
                .toList();
    }

    private RuleChangeEvent lastRuleChangeEvent() {
        List<RuleChangeEvent> events = ruleChangeEvents();
        return events.get(events.size() - 1);
    }

    private void assertChangeTypes(RuleChangeEvent.ChangeType... expected) {
        assertEquals(List.of(expected), ruleChangeEvents().stream().map(RuleChangeEvent::getChangeType).toList());
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
