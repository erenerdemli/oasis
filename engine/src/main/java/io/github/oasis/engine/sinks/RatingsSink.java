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

package io.github.oasis.engine.sinks;

import io.github.oasis.engine.external.Db;
import io.github.oasis.engine.external.DbContext;
import io.github.oasis.engine.external.Sorted;
import io.github.oasis.engine.model.ExecutionContext;
import io.github.oasis.engine.model.ID;
import io.github.oasis.engine.rules.AbstractRule;
import io.github.oasis.engine.rules.RatingRule;
import io.github.oasis.engine.rules.signals.RatingChangedSignal;
import io.github.oasis.engine.rules.signals.Signal;
import io.github.oasis.model.EventScope;

import java.io.IOException;

/**
 * @author Isuru Weerarathna
 */
public class RatingsSink extends AbstractSink {
    public RatingsSink(Db dbPool) {
        super(dbPool);
    }

    @Override
    public void consume(Signal ratingSignal, AbstractRule ratingRule, ExecutionContext context) {
        try (DbContext db = dbPool.createContext()) {
            RatingChangedSignal signal = (RatingChangedSignal) ratingSignal;
            RatingRule rule = (RatingRule) ratingRule;

            EventScope eventScope = ratingSignal.getEventScope();
            int gameId = eventScope.getGameId();
            long userId = eventScope.getUserId();
            Sorted sorted = db.SORTED(ID.getGameUserRatingsLog(gameId, userId));

            String member = signal.getRuleId() + ":"
                    + signal.getPreviousRating() + ":"
                    + signal.getCurrentRating() + ":"
                    + signal.getChangedEvent();
            sorted.add(member, signal.getOccurredTimestamp());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
