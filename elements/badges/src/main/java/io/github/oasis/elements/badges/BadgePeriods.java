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
import io.github.oasis.core.utils.Timestamps;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Objects;

/**
 * Periods a periodic badge counts within: a fixed length, or a calendar week (Monday to Sunday) or month.
 * Timestamps given here are already shifted into the player's time zone.
 */
public final class BadgePeriods {

    private static final long DAY = ChronoUnit.DAYS.getDuration().toMillis();

    private BadgePeriods() {}

    /**
     * Calendar unit of a one week or one month period, null for a fixed length period.
     */
    public static ChronoUnit calendarUnitOf(TimeUnitDef period) {
        if (Objects.isNull(period) || Objects.isNull(period.getUnit())) {
            return null;
        }
        String unit = period.getUnit().toLowerCase();
        ChronoUnit calendarUnit = unit.startsWith("w") ? ChronoUnit.WEEKS
                : unit.startsWith("mo") ? ChronoUnit.MONTHS
                : null;
        if (calendarUnit != null && !Objects.equals(period.getDuration(), 1L)) {
            throw new IllegalArgumentException("A calendar period must be one week or one month! [" + period.getDuration() + " " + period.getUnit() + "]");
        }
        return calendarUnit;
    }

    /**
     * Length of a fixed length period in milliseconds, 0 for calendar periods.
     */
    public static long lengthOf(TimeUnitDef period) {
        if (Objects.isNull(period) || calendarUnitOf(period) != null) {
            return 0;
        }
        return Timestamps.parseTimeUnit(period);
    }

    public static long startOf(long ts, long length, ChronoUnit calendarUnit) {
        if (calendarUnit == null) {
            return ts - (ts % length);
        }
        LocalDate day = LocalDate.ofEpochDay(Math.floorDiv(ts, DAY));
        LocalDate start = calendarUnit == ChronoUnit.WEEKS
                ? day.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
                : day.withDayOfMonth(1);
        return start.toEpochDay() * DAY;
    }

    public static long endOf(long start, long length, ChronoUnit calendarUnit) {
        if (calendarUnit == null) {
            return start + length;
        }
        return LocalDate.ofEpochDay(Math.floorDiv(start, DAY)).plus(1, calendarUnit).toEpochDay() * DAY;
    }
}
