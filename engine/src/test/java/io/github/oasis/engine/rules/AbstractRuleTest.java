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

import io.github.oasis.engine.model.ID;
import io.github.oasis.engine.rules.signals.BadgeSignal;
import io.github.oasis.engine.rules.signals.Signal;
import io.github.oasis.model.Event;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Isuru Weerarathna
 */
public abstract class AbstractRuleTest {

    protected static JedisPool pool;

    @BeforeClass
    public static void beforeAll() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(5);
        pool = new JedisPool(config, "localhost");
    }

    @AfterClass
    public static void afterAll() {
        pool.close();
    }

    @Before
    public void beforeEach() {
        try (Jedis jedis = pool.getResource()) {
            Set<String> keys = jedis.keys("*");
            System.out.println("Cleaning keys " + keys);
            for (String k : keys) {
                jedis.del(k);
            }
        }
    }

    @After
    public void afterEach() {
        try (Jedis jedis = pool.getResource()) {
            Map<String, String> keys = jedis.hgetAll(ID.getUserBadgesMetaKey(1, 0L));
            System.out.println("Badges: " + keys);
            String badgesKey = ID.getUserBadgeSpecKey(1, 0, "abc");
            //System.out.println(jedis.zrangeWithScores(badgesKey, 0, -1));
        }
    }

    void submitOrder(Consumer<Event> eventConsumer, Event... events) {
        for (Event event : events) {
            eventConsumer.accept(event);
        }
    }

    Set<Signal> mergeSignals(List<Signal> refSignals) {
        Set<Signal> signals = new HashSet<>();
        for (Signal signal : refSignals) {
            signals.remove(signal);
            signals.add(signal);
        }
        return signals;
    }

    void assertSignal(Collection<Signal> signals, BadgeSignal badgeSignal) {
        Assert.assertTrue("Badge not found!\n Expected: " + badgeSignal.toString(), signals.contains(badgeSignal));
        Optional<Signal> signal = signals.stream().filter(s -> s.compareTo(badgeSignal) == 0).findFirst();
        Assert.assertTrue(signal.isPresent());
    }

}
