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

import io.github.oasis.core.EventJson;
import io.github.oasis.core.elements.AbstractRule;
import io.github.oasis.core.elements.GameDef;
import io.github.oasis.core.external.messages.EngineMessage;
import io.github.oasis.core.external.messages.GameCommand;
import io.github.oasis.core.external.messages.RuleCommand;
import io.github.oasis.engine.actors.cmds.EventMessage;
import io.github.oasis.engine.actors.cmds.Messages;
import io.github.oasis.engine.actors.cmds.OasisRuleMessage;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Isuru Weerarathna
 */
class DefinitionReader {

    private static final Map<String, RuleCommand.RuleChangeType> RULE_CHANGE_TYPE_MAP = Map.of(
        EngineMessage.GAME_RULE_ADDED, RuleCommand.RuleChangeType.ADD,
        EngineMessage.GAME_RULE_REMOVED, RuleCommand.RuleChangeType.REMOVE,
        EngineMessage.GAME_RULE_UPDATED, RuleCommand.RuleChangeType.UPDATE
    );

    static List<AbstractRule> parseDefsToRules(int gameId, GameDef gameDef, EngineContext context) {
        return gameDef.getRuleDefinitions()
                .stream()
                .map(def -> context.getParsers().parseToRule(def))
                .collect(Collectors.toList());
    }

    static Object derive(EngineMessage def, EngineContext context) {
        if (def.isEvent()) {
            return new EventMessage(new EventJson(def.getData()), null, def.getMessageId());
        } else if (def.isGameLifecycleEvent()) {
            GameCommand cmd = new GameCommand();
            cmd.setMessageId(def.getMessageId());
            cmd.setGameId(def.getScope().getGameId());
            cmd.setStatus(toLifecycleType(def.getType()));
            return cmd;
        } else if (def.isRuleEvent()) {
            return readRuleMessage(def, context);
        }
        return null;
    }

    private static OasisRuleMessage readRuleMessage(EngineMessage def, EngineContext context) {
        int gameId = def.getScope().getGameId();
        RuleCommand.RuleChangeType ruleChangeType = toRuleChangeType(def.getType());
        AbstractRule rule = context.getParsers().parseToRule(def);
        Object messageId = def.getMessageId();
        if (ruleChangeType == RuleCommand.RuleChangeType.ADD) {
            return Messages.createRuleAddMessage(gameId, rule, messageId);
        } else if (ruleChangeType == RuleCommand.RuleChangeType.REMOVE) {
            return Messages.createRuleRemoveMessage(gameId, rule.getId(), messageId);
        } else {
            return Messages.createRuleUpdateMessage(gameId, rule, messageId);
        }
    }

    private static RuleCommand.RuleChangeType toRuleChangeType(String type) {
        return Optional.ofNullable(RULE_CHANGE_TYPE_MAP.get(type))
                .orElseThrow(() -> new IllegalArgumentException("Unknown rule change type! [" + type + "]"));
    }

    private static GameCommand.GameLifecycle toLifecycleType(String type) {
        switch (type) {
            case EngineMessage.GAME_CREATED: return GameCommand.GameLifecycle.CREATE;
            case EngineMessage.GAME_PAUSED: return GameCommand.GameLifecycle.PAUSE;
            case EngineMessage.GAME_REMOVED: return GameCommand.GameLifecycle.REMOVE;
            case EngineMessage.GAME_STARTED: return GameCommand.GameLifecycle.START;
            case EngineMessage.GAME_UPDATED: return GameCommand.GameLifecycle.UPDATE;
            default: throw new IllegalArgumentException("Unknown game lifecycle type! [" + type + "]");
        }
    }
}
