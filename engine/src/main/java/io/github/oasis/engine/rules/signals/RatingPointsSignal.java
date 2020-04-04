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

package io.github.oasis.engine.rules.signals;

import io.github.oasis.model.Event;
import lombok.ToString;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Objects;

/**
 * @author Isuru Weerarathna
 */
@ToString(callSuper = true)
public class RatingPointsSignal extends AbstractRatingSignal {

    private BigDecimal points;
    private Event causedEvent;

    public RatingPointsSignal(String ruleId, int currentRating, BigDecimal points, Event causedEvent) {
        super(ruleId, currentRating);
        this.points = points;
        this.causedEvent = causedEvent;
    }

    public BigDecimal getPoints() {
        return points;
    }

    public Event getCausedEvent() {
        return causedEvent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RatingPointsSignal that = (RatingPointsSignal) o;
        return getRuleId().equals(that.getRuleId()) &&
                getCurrentRating() == that.getCurrentRating() &&
                points.equals(that.points) &&
                causedEvent.equals(that.causedEvent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRuleId(), getCurrentRating(), points, causedEvent);
    }

    @Override
    public int compareTo(Signal o) {
        if (o instanceof RatingPointsSignal) {
            return Comparator.comparing(RatingPointsSignal::getRuleId)
                    .thenComparing(RatingPointsSignal::getCurrentRating)
                    .thenComparing(RatingPointsSignal::getPoints)
                    .thenComparing(o2 -> o2.causedEvent.getExternalId())
                    .compare(this, (RatingPointsSignal) o);
        }
        return -1;
    }
}
