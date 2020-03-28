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

package io.github.oasis.engine.rules;

import io.github.oasis.engine.rules.signals.Signal;
import io.github.oasis.model.Event;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author Isuru Weerarathna
 */
public class ConditionalBadgeRule extends BadgeRule {

    private int maxAwardTimes = Integer.MAX_VALUE;
    private List<Condition> conditions;
    private Consumer<Signal> collector;

    public ConditionalBadgeRule(String id) {
        super(id);
    }

    public int getMaxAwardTimes() {
        return maxAwardTimes;
    }

    public void setMaxAwardTimes(int maxAwardTimes) {
        this.maxAwardTimes = maxAwardTimes;
    }

    public Consumer<Signal> getCollector() {
        return collector;
    }

    public void setCollector(Consumer<Signal> collector) {
        this.collector = collector;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(List<Condition> conditions) {
        this.conditions = new LinkedList<>(conditions);
        Collections.sort(this.conditions);
    }

    public static class Condition implements Comparable<Condition> {
        private int priority;
        private Predicate<Event> condition;
        private int attribute;

        public Condition(int priority, Predicate<Event> condition, int attribute) {
            this.priority = priority;
            this.condition = condition;
            this.attribute = attribute;
        }

        public int getPriority() {
            return priority;
        }

        public Predicate<Event> getCondition() {
            return condition;
        }

        public int getAttribute() {
            return attribute;
        }

        @Override
        public int compareTo(Condition o) {
            return Integer.compare(this.getPriority(), o.getPriority());
        }
    }

}