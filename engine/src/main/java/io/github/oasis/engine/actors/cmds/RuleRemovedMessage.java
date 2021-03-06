/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.oasis.engine.actors.cmds;

import io.github.oasis.engine.ext.RulesImpl;

/**
 * @author Isuru Weerarathna
 */
public class RuleRemovedMessage extends OasisRuleMessage {

    private String ruleId;

    RuleRemovedMessage(String ruleId, Object messageId) {
        super(messageId);
        this.ruleId = ruleId;
    }

    public String getRuleId() {
        return ruleId;
    }

    @Override
    public void applyTo(RulesImpl.GameRules gameRules) {
        gameRules.removeRule(ruleId);
    }

    @Override
    public String toString() {
        return "RuleAdded{" +
                "game=" + getGameId() + ", " +
                "rule=" + ruleId + ", " +
                "messageId=" + getExternalMessageId() +
                '}';
    }
}
