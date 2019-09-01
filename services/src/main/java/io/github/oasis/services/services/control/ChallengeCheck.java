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

package io.github.oasis.services.services.control;

import io.github.oasis.model.Event;
import io.github.oasis.model.defs.ChallengeDef;
import io.github.oasis.services.utils.Commons;
import org.apache.commons.lang3.BooleanUtils;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ChallengeCheck implements Serializable {

    private static final ChallengeFilterResult CONTINUE =
            new ChallengeFilterResult(false, true, 0);
    private static final ChallengeFilterResult HALT =
            new ChallengeFilterResult(false, false, 0);

    private ChallengeDef def;
    private Set<String> eventNames;
    private List<Serializable> conditions;
    private int winners;

    public ChallengeCheck() {}

    public ChallengeCheck(ChallengeDef def) {
        this.def = def;
        this.eventNames = new HashSet<>(def.getForEvents());
        this.winners = 0;

        this.conditions = new LinkedList<>();
        if (!Commons.isNullOrEmpty(def.getConditions())) {
            for (String expr : def.getConditions()) {
                this.conditions.add(MVEL.compileExpression(expr));
            }
        }
    }

    public ChallengeFilterResult check(Event event) {
        if (!eventNames.contains(event.getEventType())) {
            return CONTINUE;
        }

        if (winners >= def.getWinnerCount()) {
            return HALT;
        }

        // check for expiration
        if (event.getTimestamp() > def.getExpireAfter()) {
            return HALT;
        } else if (event.getTimestamp() < def.getStartAt()) {
            return CONTINUE;
        }

        // check user match
        if (def.getForUserId() != null && event.getUser() != def.getForUserId()) {
            return CONTINUE;
        }

        // check team match
        if (def.getForTeamId() != null && !def.getForTeamId().equals(event.getTeam())) {
            return CONTINUE;
        }

        // check team-scope match
        if (def.getForTeamScopeId() != null && !def.getForTeamScopeId().equals(event.getTeamScope())) {
            return CONTINUE;
        }

        boolean satisfied = conditions.size() == 0;
        Map<String, Object> variables = new HashMap<>(event.getAllFieldValues());
        for (Serializable expr : conditions) {
            if (interpretCondition(MVEL.executeExpression(expr, variables))) {
                satisfied = true;
                break;
            }
        }

        if (satisfied) {
            winners++;
        }

        boolean canContinue = def.getWinnerCount() > winners;
        return new ChallengeFilterResult(satisfied, !satisfied || canContinue, winners);
    }

    private boolean interpretCondition(Object val) {
        if (val == null) return false;
        if (val instanceof Boolean) {
            return (boolean) val;
        } else if (val instanceof Number) {
            return ((Number) val).longValue() > 0;
        }
        return BooleanUtils.toBoolean(val.toString());
    }

    public ChallengeDef getDef() {
        return def;
    }

    public Set<String> getEventNames() {
        return new HashSet<>(eventNames);
    }

    public List<Serializable> getConditions() {
        return conditions;
    }

    static class ChallengeFilterResult {
        private final boolean satisfied;
        private final boolean isContinue;
        private final int winNumber;

        private ChallengeFilterResult(boolean satisfied, boolean isContinue, int winNumber) {
            this.satisfied = satisfied;
            this.isContinue = isContinue;
            this.winNumber = winNumber;
        }

        int getWinNumber() {
            return winNumber;
        }

        boolean isSatisfied() {
            return satisfied;
        }

        boolean isContinue() {
            return isContinue;
        }
    }

}