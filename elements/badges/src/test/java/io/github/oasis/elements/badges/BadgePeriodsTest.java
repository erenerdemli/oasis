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

package io.github.oasis.elements.badges;

import io.github.oasis.core.elements.spec.TimeUnitDef;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BadgePeriodsTest {

    @Test
    void weeksRunMondayToSunday() {
        long wednesday = ms("2026-11-04T10:00:00Z");
        long start = BadgePeriods.startOf(wednesday, 0, ChronoUnit.WEEKS);

        assertEquals(ms("2026-11-02T00:00:00Z"), start);
        assertEquals(ms("2026-11-09T00:00:00Z"), BadgePeriods.endOf(start, 0, ChronoUnit.WEEKS));
        assertEquals(start, BadgePeriods.startOf(ms("2026-11-08T23:59:59Z"), 0, ChronoUnit.WEEKS));
        assertEquals(ms("2026-11-09T00:00:00Z"), BadgePeriods.startOf(ms("2026-11-09T00:00:00Z"), 0, ChronoUnit.WEEKS));
    }

    @Test
    void monthsAreCalendarMonths() {
        long start = BadgePeriods.startOf(ms("2024-02-15T08:30:00Z"), 0, ChronoUnit.MONTHS);

        assertEquals(ms("2024-02-01T00:00:00Z"), start);
        assertEquals(ms("2024-03-01T00:00:00Z"), BadgePeriods.endOf(start, 0, ChronoUnit.MONTHS));
    }

    @Test
    void fixedLengthsAreUnchanged() {
        long ts = ms("2026-11-04T10:00:00Z");
        long day = ChronoUnit.DAYS.getDuration().toMillis();

        assertEquals(ts - (ts % day), BadgePeriods.startOf(ts, day, null));
        assertEquals(ts - (ts % day) + day, BadgePeriods.endOf(ts - (ts % day), day, null));
    }

    @Test
    void readsPeriodDefinitions() {
        assertEquals(ChronoUnit.WEEKS, BadgePeriods.calendarUnitOf(TimeUnitDef.of(1L, "weekly")));
        assertEquals(ChronoUnit.MONTHS, BadgePeriods.calendarUnitOf(TimeUnitDef.of(1L, "month")));
        assertNull(BadgePeriods.calendarUnitOf(TimeUnitDef.of(1L, "days")));
        assertEquals(0, BadgePeriods.lengthOf(TimeUnitDef.of(1L, "weeks")));
        assertEquals(86400000L, BadgePeriods.lengthOf(TimeUnitDef.of(1L, "days")));
        assertThrows(IllegalArgumentException.class, () -> BadgePeriods.calendarUnitOf(TimeUnitDef.of(2L, "weeks")));
    }

    private static long ms(String instant) {
        return Instant.parse(instant).toEpochMilli();
    }
}
