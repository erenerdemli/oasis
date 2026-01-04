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

package io.github.oasis.core.services.api.handlers;

import io.github.oasis.core.services.api.services.IEngineManager;
import io.github.oasis.core.services.api.services.impl.ElementService;
import io.github.oasis.core.services.events.RuleChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Listens to all rule change events produced by {@link ElementService}.
 * 
 * <p>Having this listener separately removes a circular dependency between
 * {@link ElementService} and {@link IEngineManager} services, following
 * the same pattern as {@link GameStatusListener}.</p>
 *
 * <p>When a rule is added, updated, or removed via the API, this listener
 * forwards the change to the engine manager, which notifies running game
 * engines. This enables real-time rule updates without requiring a game restart.</p>
 *
 * @author Isuru Weerarathna
 */
@Component
public class RuleChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(RuleChangeListener.class);

    private final IEngineManager engineManager;

    public RuleChangeListener(IEngineManager engineManager) {
        this.engineManager = engineManager;
    }

    /**
     * Handles rule change events and notifies the engine manager.
     * 
     * <p>This method is called asynchronously by Spring's event mechanism
     * whenever a {@link RuleChangeEvent} is published.</p>
     *
     * @param event the rule change event containing change type and element details
     */
    @EventListener
    public void handleRuleChangeEvent(RuleChangeEvent event) {
        LOG.debug("Received rule change event: type={}, gameId={}, elementId={}",
                event.getChangeType(), event.getGameId(), 
                event.getElementDef() != null ? event.getElementDef().getElementId() : "null");

        try {
            engineManager.notifyRuleChange(
                    event.getChangeType(),
                    event.getGameId(),
                    event.getElementDef()
            );
        } catch (Exception e) {
            // Log the error but don't rethrow - the database change has already been committed
            // The rule change will be picked up when the game is restarted
            LOG.error("Failed to notify engine of rule change: type={}, gameId={}, elementId={}. " +
                            "The change will take effect when the game is restarted.",
                    event.getChangeType(), event.getGameId(),
                    event.getElementDef() != null ? event.getElementDef().getElementId() : "null", e);
        }
    }
}
