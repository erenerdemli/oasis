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

import io.github.oasis.core.services.AbstractStatsApiResponse;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Answer to {@link UserPointsBulkRequest}: one row per REQUESTED user.
 *
 * <p>A user with no points yet is returned with zero rather than omitted, so a
 * caller can join on the response without having to decide whether a missing
 * row means "none" or "lost".</p>
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class UserPointsBulkSummary extends AbstractStatsApiResponse {

    private List<UserPoints> users = new ArrayList<>();

    public void addUser(Long userId, BigDecimal totalPoints) {
        users.add(new UserPoints(userId, totalPoints));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UserPoints {
        private Long userId;
        private BigDecimal totalPoints;
    }
}
