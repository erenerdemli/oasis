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

package io.github.oasis.core.elements.matchers;

import io.github.oasis.core.elements.TimeRangeMatcher;

import java.util.List;

/**
 * This class will take multiple time ranges and will return true only
 * if all of them are satisfied.
 *
 * @author Isuru Weerarathna
 */
public class AllOfTimeRangeMatcher implements TimeRangeMatcher {

    private final List<TimeRangeMatcher> matcherList;

    public AllOfTimeRangeMatcher(List<TimeRangeMatcher> matcherList) {
        this.matcherList = matcherList;
    }

    @Override
    public boolean isBetween(long timeMs, String timeZone) {
        return matcherList.stream()
                .allMatch(mat -> mat.isBetween(timeMs, timeZone));
    }
}