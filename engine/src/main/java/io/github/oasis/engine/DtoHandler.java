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

package io.github.oasis.engine;

import io.github.oasis.core.elements.AbstractRule;
import io.github.oasis.core.external.messages.GameCommand;
import io.github.oasis.core.external.messages.PersistedDef;
import io.github.oasis.core.external.messages.RuleCommand;
import io.github.oasis.engine.actors.cmds.RuleAddedMessage;
import io.github.oasis.engine.actors.cmds.RuleRemovedMessage;
import io.github.oasis.engine.actors.cmds.RuleUpdatedMessage;

/**
 * @author Isuru Weerarathna
 */
class DtoHandler {

    static Object derive(PersistedDef def, EngineContext context) {
        if (def.isEvent()) {
            return new EventJson(def.getData());
        } else if (def.isGameLifecycleEvent()) {
            GameCommand cmd = new GameCommand();
            cmd.setMessageId(def.getMessageId());
            cmd.setGameId(def.getScope().getGameId());
            cmd.setStatus(toLifecycleType(def.getType()));
            return cmd;
        } else if (def.isRuleEvent()) {
            int gameId = def.getScope().getGameId();
            RuleCommand.RuleChangeType ruleChangeType = toRuleChangeType(def.getType());
            AbstractRule rule = context.getParsers().parseToRule(def);
            if (ruleChangeType == RuleCommand.RuleChangeType.ADD) {
                return RuleAddedMessage.create(gameId, rule);
            } else if (ruleChangeType == RuleCommand.RuleChangeType.REMOVE) {
                return RuleRemovedMessage.create(gameId, rule.getId());
            } else {
                return RuleUpdatedMessage.create(gameId, rule);
            }
        }
        return null;
    }

    private static RuleCommand.RuleChangeType toRuleChangeType(String type) {
        switch (type) {
            case PersistedDef.GAME_RULE_ADDED: return RuleCommand.RuleChangeType.ADD;
            case PersistedDef.GAME_RULE_REMOVED: return RuleCommand.RuleChangeType.REMOVE;
            case PersistedDef.GAME_RULE_UPDATED: return RuleCommand.RuleChangeType.UPDATE;
            default: throw new IllegalArgumentException("Unknown rule change type! [" + type + "]");
        }
    }

    private static GameCommand.GameLifecycle toLifecycleType(String type) {
        switch (type) {
            case PersistedDef.GAME_ADDED: return GameCommand.GameLifecycle.CREATE;
            case PersistedDef.GAME_PAUSED: return GameCommand.GameLifecycle.PAUSE;
            case PersistedDef.GAME_REMOVED: return GameCommand.GameLifecycle.REMOVE;
            case PersistedDef.GAME_STARTED: return GameCommand.GameLifecycle.START;
            case PersistedDef.GAME_UPDATED: return GameCommand.GameLifecycle.UPDATE;
            default: throw new IllegalArgumentException("Unknown game lifecycle type! [" + type + "]");
        }
    }
}
