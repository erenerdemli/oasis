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

package io.github.oasis.elements.badges.processors;

import io.github.oasis.core.elements.RuleContext;
import io.github.oasis.core.elements.Signal;
import io.github.oasis.core.elements.matchers.SingleEventTypeMatcher;
import io.github.oasis.elements.badges.TEvent;
import io.github.oasis.elements.badges.rules.PeriodicBadgeRule;
import io.github.oasis.elements.badges.signals.BadgeRemoveSignal;
import io.github.oasis.elements.badges.signals.TemporalBadgeSignal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author Isuru Weerarathna
 */
@DisplayName("Periodic Threshold Badges")
public class PeriodicBadgeTest extends AbstractRuleTest {

    private static final int ATTR_1 = 1;
    private static final int ATTR_2 = 2;
    private static final int ATTR_4 = 4;

    private static final long FIFTY = 50;
    public static final String EVENT_TYPE = "reputation.changed";
    public static final String EVENT_TYPE_B = "unknown.event";

    @DisplayName("No Thresholds")
    @Test
    public void testNoThresholds() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 112);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 14);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 4);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 5);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 84);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(FIFTY, signals);
        Assertions.assertEquals(0, ruleContext.getRule().getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        Assertions.assertEquals(0, signals.size());
    }

    @DisplayName("Single Threshold: no threshold passed")
    @Test
    public void testSingleT() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 12);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 14);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 4);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 5);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "SINGLE_THRESHOLD");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        Assertions.assertEquals(1, rule.getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        Assertions.assertEquals(0, signals.size());
    }

    @DisplayName("Single Threshold: no matching events")
    @Test
    public void testSingleTNoMatchEvents() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE_B, 75);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE_B, 63);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE_B, 99);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE_B, 26);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE_B, 96);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE_B, 71);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "SINGLE_THRESHOLD");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        Assertions.assertEquals(0, signals.size());
    }

    @DisplayName("Single Threshold: badge creation")
    @Test
    public void testSingleTBadge() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 99);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 54);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 43);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 5);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "SINGLE_THRESHOLD");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()));
    }

    @DisplayName("Single Threshold: badge creation with filter")
    @Test
    public void testSingleTBadgeWithC() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 99);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 23);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 43);
        TEvent e4 = TEvent.createKeyValue(125, EVENT_TYPE, 46);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "SINGLE_THRESHOLD_WITH_FILTER");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        Assertions.assertEquals(1, rule.getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e4, ATTR_1, 100, 150, e4.getTimestamp(), e4.getExternalId()));
    }

    @DisplayName("Single Threshold: badges in different time units")
    @Test
    public void testSingleTBadgeInDiffT() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 99);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 54);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 43);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 55);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "SINGLE_THRESHOLD");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        Assertions.assertEquals(1, rule.getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e6, ATTR_1, 150, 200, e6.getTimestamp(), e6.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()));
    }

    @DisplayName("Single Threshold: Out-of-order badge creation")
    @Test
    public void testSingleTOOOBadge() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 54);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 44);
        TEvent e3 = TEvent.createKeyValue(155, EVENT_TYPE, 55);
        TEvent e4 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e5 = TEvent.createKeyValue(165, EVENT_TYPE, 39);
        TEvent e6 = TEvent.createKeyValue(120, EVENT_TYPE, 43);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "SINGLE_THRESHOLD");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e5, ATTR_1, 150, 200, e5.getTimestamp(), e5.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e6, ATTR_1, 100, 150, e6.getTimestamp(), e6.getExternalId()));
    }

    @DisplayName("Single Threshold: Out-of-order badge removal")
    @Test
    public void testSingleTOOOBadgeRemoval() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 54);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 82);
        TEvent e3 = TEvent.createKeyValue(155, EVENT_TYPE, 55);
        TEvent e4 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e5 = TEvent.createKeyValue(165, EVENT_TYPE, 39);
        TEvent e6 = TEvent.createKeyValue(120, EVENT_TYPE, -43);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "SINGLE_THRESHOLD");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e5, ATTR_1, 150, 200, e5.getTimestamp(), e5.getExternalId()),
                new BadgeRemoveSignal(rule.getId(), e6.asEventScope(), ATTR_1, 100, 150, e6.getExternalId(), e6.getExternalId()));
    }

    // ------------------------------------------------------
    // MULTIPLE THRESHOLDS
    // ------------------------------------------------------

    @DisplayName("Multi Threshold: no threshold passed")
    @Test
    public void testMultiT() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 12);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 14);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 4);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 5);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "MULTIPLE_THRESHOLDS");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        Assertions.assertEquals(3, rule.getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        Assertions.assertEquals(0, signals.size());
    }

    @DisplayName("Multi Threshold: no matching passed")
    @Test
    public void testMultiTNoMatches() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE_B, 75);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE_B, 63);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE_B, 99);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE_B, 26);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE_B, 96);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE_B, 71);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "MULTIPLE_THRESHOLDS");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        Assertions.assertEquals(3, rule.getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        Assertions.assertEquals(0, signals.size());
    }

    @DisplayName("Multi Threshold: first badge creation")
    @Test
    public void testMultiTFirstBadge() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 79);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 54);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 13);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 5);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "MULTIPLE_THRESHOLDS");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        Assertions.assertEquals(3, rule.getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()));
    }

    @DisplayName("Multi Threshold: many badges creation")
    @Test
    public void testMultiTMultiBadge() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 89);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 54);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 13);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 5);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "MULTIPLE_THRESHOLDS");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e3, ATTR_2, 100, 150, e3.getTimestamp(), e3.getExternalId()));
    }

    @DisplayName("Multi Threshold: many badges creation to exact threshold")
    @Test
    public void testMultiTMultiBadgeExactT() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 89);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 11);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 51);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 5);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "MULTIPLE_THRESHOLDS");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e3, ATTR_2, 100, 150, e3.getTimestamp(), e3.getExternalId()));
    }

    @DisplayName("Multi Threshold: many badges creation with condition")
    @Test
    public void testMultiTMultiBadgeWithC() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 89);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 54);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 13);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 5);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "MULTIPLE_THRESHOLDS_WITH_FILTER");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        Assertions.assertEquals(3, rule.getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()));
    }

    @DisplayName("Multi Threshold: many badges in different time unit creation")
    @Test
    public void testMultiTMultiBadgeDiffT() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 89);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 54);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 13);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 45);
        TEvent e5 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e6 = TEvent.createKeyValue(165, EVENT_TYPE, 39);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "MULTIPLE_THRESHOLDS");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        Assertions.assertEquals(3, rule.getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e3, ATTR_2, 100, 150, e3.getTimestamp(), e3.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e6, ATTR_1, 150, 200, e6.getTimestamp(), e6.getExternalId()));
    }

    @DisplayName("Multi Threshold: jumped badges creation")
    @Test
    public void testMultiTMultiBadgeJumpBadges() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 99);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 64);
        TEvent e3 = TEvent.createKeyValue(120, EVENT_TYPE, 13);
        TEvent e4 = TEvent.createKeyValue(155, EVENT_TYPE, 5);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "MULTIPLE_THRESHOLDS");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        Assertions.assertEquals(3, rule.getThresholds().size());
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_2, 100, 150, e2.getTimestamp(), e2.getExternalId()));
    }

    @DisplayName("Multi Threshold: Out-of-order badges creation")
    @Test
    public void testMultiTOOOMultiBadgeDiffT() {
        TEvent e1 = TEvent.createKeyValue(110, EVENT_TYPE, 89);
        TEvent e2 = TEvent.createKeyValue(115, EVENT_TYPE, 54);
        TEvent e3 = TEvent.createKeyValue(155, EVENT_TYPE, 45);
        TEvent e4 = TEvent.createKeyValue(160, EVENT_TYPE, 34);
        TEvent e5 = TEvent.createKeyValue(165, EVENT_TYPE, 39);
        TEvent e6 = TEvent.createKeyValue(120, EVENT_TYPE, 13);

        List<Signal> signals = new ArrayList<>();
        PeriodicBadgeRule rule = loadRule("kinds/periodic.yml", "MULTIPLE_THRESHOLDS");
        RuleContext<PeriodicBadgeRule> ruleContext = createRule(rule, signals);
        PeriodicBadgeProcessor processor = new PeriodicBadgeProcessor(pool, ruleContext);
        submitOrder(processor, e1, e2, e3, e4, e5, e6);

        System.out.println(signals);
        assertStrict(signals,
                new TemporalBadgeSignal(rule.getId(), e2, ATTR_1, 100, 150, e2.getTimestamp(), e2.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e6, ATTR_2, 100, 150, e6.getTimestamp(), e6.getExternalId()),
                new TemporalBadgeSignal(rule.getId(), e5, ATTR_1, 150, 200, e5.getTimestamp(), e5.getExternalId()));
    }

    private RuleContext<PeriodicBadgeRule> createRule(long timeUnit, Collection<Signal> collection, PeriodicBadgeRule.Threshold... thresholds) {
        PeriodicBadgeRule rule = new PeriodicBadgeRule("test.temporal.badge");
        rule.setEventTypeMatcher(new SingleEventTypeMatcher(EVENT_TYPE));
        rule.setTimeUnit(timeUnit);
        rule.setValueResolver((e,c) -> new BigDecimal(e.getFieldValue("value").toString()));
        rule.setThresholds(Arrays.asList(thresholds));
        return new RuleContext<>(rule, fromConsumer(collection::add));
    }

    private RuleContext<PeriodicBadgeRule> createRule(PeriodicBadgeRule rule, Collection<Signal> collection) {
        return new RuleContext<>(rule, fromConsumer(collection::add));
    }
}
