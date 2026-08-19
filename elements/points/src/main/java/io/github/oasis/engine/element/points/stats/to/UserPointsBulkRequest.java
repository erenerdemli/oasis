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

package io.github.oasis.engine.element.points.stats.to;

import io.github.oasis.core.services.AbstractStatsApiRequest;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Total points for MANY users of one game in a single call.
 *
 * <p>The per-user endpoint reads one Redis hash field, so the cost of asking
 * about a list of users is dominated by the HTTP round-trip, not the lookup.
 * A caller rendering a roster — a participant list, a leaderboard of a known
 * cohort — would otherwise issue one request per row.</p>
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class UserPointsBulkRequest extends AbstractStatsApiRequest {

    /**
     * Upper bound on one request. Each id costs a hash read on the connection,
     * and the game's keys share a hash tag so they stay on one node; this exists
     * to keep a single request from monopolising it.
     */
    public static final int MAX_USERS = 1000;

    private List<Long> userIds;
}
