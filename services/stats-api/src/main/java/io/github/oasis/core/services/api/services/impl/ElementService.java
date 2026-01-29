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

package io.github.oasis.core.services.api.services.impl;

import io.github.oasis.core.Game;
import io.github.oasis.core.elements.ElementDef;
import io.github.oasis.core.elements.SimpleElementDefinition;
import io.github.oasis.core.exception.OasisParseException;
import io.github.oasis.core.external.OasisRepository;
import io.github.oasis.core.services.annotations.AdminDbRepository;
import io.github.oasis.core.services.api.beans.StatsApiContext;
import io.github.oasis.core.services.api.exceptions.ErrorCodes;
import io.github.oasis.core.services.api.exceptions.OasisApiRuntimeException;
import io.github.oasis.core.services.api.services.IElementService;
import io.github.oasis.core.services.api.to.ElementCreateRequest;
import io.github.oasis.core.services.api.to.ElementUpdateRequest;
import io.github.oasis.core.services.events.RuleChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Service for managing game elements (rules) such as badges, points, milestones, etc.
 * 
 * <p>This service handles CRUD operations for game elements and publishes
 * {@link RuleChangeEvent}s to notify the engine of changes to running games.</p>
 *
 * @author Isuru Weerarathna
 */
@Service
public class ElementService extends AbstractOasisService implements IElementService {

    private static final Logger LOG = LoggerFactory.getLogger(ElementService.class);

    private final StatsApiContext statsApiContext;
    private final ApplicationEventPublisher eventPublisher;

    public ElementService(@AdminDbRepository OasisRepository backendRepository, 
                         StatsApiContext statsApiContext,
                         ApplicationEventPublisher eventPublisher) {
        super(backendRepository);
        this.statsApiContext = statsApiContext;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public ElementDef readElement(int gameId, String elementId, boolean withData) {
        ElementDef def;
        if (withData) {
            def = backendRepository.readElement(gameId, elementId);
        } else {
            def = backendRepository.readElementWithoutData(gameId, elementId);
        }
        return Optional.ofNullable(def)
                .orElseThrow(() -> new OasisApiRuntimeException(
                        ErrorCodes.ELEMENT_NOT_EXISTS,
                        HttpStatus.NOT_FOUND));
    }

    @Override
    public ElementDef addElement(int gameId, ElementCreateRequest request) throws OasisParseException {
        ElementDef elementDef = ElementDef.builder()
                .data(request.getData())
                .gameId(request.getGameId())
                .elementId(request.getMetadata().getId())
                .metadata(request.getMetadata().toElementDefinition())
                .type(request.getType())
                .build();

        try {
            statsApiContext.validateElement(elementDef);

            ElementDef savedElement = backendRepository.addNewElement(gameId, elementDef);

            // Publish rule change event to notify running game engines
            // The listener will check if the game is running before forwarding to engine
            publishRuleChangeEvent(RuleChangeEvent.ChangeType.ADD, gameId, savedElement);
            LOG.info("Element added: gameId={}, elementId={}", gameId, savedElement.getElementId());

            return savedElement;
        } catch (RuntimeException e) {
            var metadata = request.getMetadata();
            LOG.warn("Failed to add element: gameId={}, elementId={}, type={}, name={}",
                    gameId,
                    metadata != null ? metadata.getId() : null,
                    request.getType(),
                    metadata != null ? metadata.getName() : null,
                    e);
            throw e;
        }
    }

    @Override
    public ElementDef updateElement(int gameId, String elementId, ElementUpdateRequest updateRequest) {
        SimpleElementDefinition metadata = new SimpleElementDefinition(
                elementId,
                updateRequest.getName(),
                updateRequest.getDescription(),
                updateRequest.getIconUrl(),
                updateRequest.getWeight(),
                updateRequest.getVersion());
        ElementDef updatedElement = backendRepository.updateElement(gameId, elementId, metadata);

        // For updates, we need the full element data to send to the engine
        // Read the element with data to ensure engine gets complete rule definition
        ElementDef elementWithData = backendRepository.readElement(gameId, elementId);

        // Publish rule change event to notify running game engines
        publishRuleChangeEvent(RuleChangeEvent.ChangeType.UPDATE, gameId, elementWithData);
        LOG.info("Element updated: gameId={}, elementId={}", gameId, elementId);

        return updatedElement;
    }

    @Override
    public ElementDef deleteElement(int gameId, String elementId) {
        // Read the element before deletion to have the data for the event
        ElementDef elementToDelete = backendRepository.readElement(gameId, elementId);
        
        ElementDef deletedElement = backendRepository.deleteElement(gameId, elementId);

        // Publish rule change event to notify running game engines
        // Use the element data we read before deletion
        if (elementToDelete != null) {
            publishRuleChangeEvent(RuleChangeEvent.ChangeType.REMOVE, gameId, elementToDelete);
            LOG.info("Element deleted: gameId={}, elementId={}", gameId, elementId);
        }

        return deletedElement;
    }

    /**
     * Publishes a rule change event to notify listeners (and ultimately the engine)
     * about changes to game rules.
     *
     * @param changeType the type of change (ADD, UPDATE, or REMOVE)
     * @param gameId     the game ID
     * @param elementDef the element definition
     */
    private void publishRuleChangeEvent(RuleChangeEvent.ChangeType changeType, int gameId, ElementDef elementDef) {
        RuleChangeEvent event = new RuleChangeEvent(changeType, gameId, elementDef);
        eventPublisher.publishEvent(event);
        LOG.debug("Published rule change event: type={}, gameId={}, elementId={}",
                changeType, gameId, elementDef.getElementId());
    }

    @Override
    public List<ElementDef> listElementsByType(int gameId, String type) {
        return backendRepository.readElementsByType(gameId, type);
    }

    @Override
    public List<ElementDef> listElementsFromGameId(int gameId) {
        Game game = backendRepository.readGame(gameId);
        if (game == null) {
            throw new OasisApiRuntimeException(ErrorCodes.GAME_NOT_EXISTS, HttpStatus.NOT_FOUND);
        }

        return backendRepository.readElementsByGameId(gameId);
    }
}
