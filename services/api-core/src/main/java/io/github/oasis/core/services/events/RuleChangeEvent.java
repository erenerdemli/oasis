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

package io.github.oasis.core.services.events;

import io.github.oasis.core.elements.ElementDef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Indicates an event when a game rule (element) is added, updated, or removed.
 * This event is published by {@link io.github.oasis.core.services.api.services.IElementService}
 * and consumed by the engine manager to notify running game engines of rule changes.
 *
 * <p>This enables real-time rule updates without requiring a game restart.</p>
 *
 * @author Isuru Weerarathna
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleChangeEvent implements Serializable {

    /**
     * The type of change that occurred to the rule.
     */
    public enum ChangeType {
        /**
         * A new rule was added to the game.
         */
        ADD,
        /**
         * An existing rule was updated.
         */
        UPDATE,
        /**
         * An existing rule was removed from the game.
         */
        REMOVE
    }

    /**
     * The type of change (ADD, UPDATE, or REMOVE).
     */
    private ChangeType changeType;

    /**
     * The game ID to which this rule belongs.
     */
    private int gameId;

    /**
     * The element definition containing rule details.
     * For REMOVE operations, only the elementId field is required.
     */
    private ElementDef elementDef;

}
