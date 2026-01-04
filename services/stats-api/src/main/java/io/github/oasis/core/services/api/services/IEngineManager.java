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
import io.github.oasis.core.external.messages.GameState;
import io.github.oasis.core.services.api.exceptions.EngineManagerException;
import io.github.oasis.core.services.events.RuleChangeEvent;

/**
 * Interface to manipulate game engine thorough API.
 *
 * @author Isuru Weerarathna
 */
public interface IEngineManager {

    /**
     * Notifies the engine about a game status change (e.g., started, paused, stopped).
     *
     * @param state the new game state
     * @param game  the game reference
     * @throws EngineManagerException if notification fails
     */
    void notifyGameStatusChange(GameState state, Game game) throws EngineManagerException;

    /**
     * Notifies the engine about a rule change (add, update, or remove).
     * This method only sends the notification if the associated game is currently running.
     * If the game is not running, the change will be picked up when the game starts.
     *
     * @param changeType  the type of change (ADD, UPDATE, or REMOVE)
     * @param gameId      the game ID to which the rule belongs
     * @param elementDef  the element definition (rule details)
     * @throws EngineManagerException if notification fails
     */
    void notifyRuleChange(RuleChangeEvent.ChangeType changeType, int gameId, ElementDef elementDef) throws EngineManagerException;

}
