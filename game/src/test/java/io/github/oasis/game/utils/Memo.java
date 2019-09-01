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

package io.github.oasis.game.utils;

import io.github.oasis.model.Badge;
import io.github.oasis.model.Event;
import io.github.oasis.model.Milestone;
import io.github.oasis.model.events.ChallengeEvent;
import io.github.oasis.model.rules.BadgeRule;
import io.github.oasis.model.rules.PointRule;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Memo {

    private static final Map<String, List<Tuple4<Long, List<? extends Event>, PointRule, Double>>> pointsMap = new ConcurrentHashMap<>();
    private static final Map<String, List<Tuple4<Long, List<? extends Event>, Badge, BadgeRule>>> badgeMap = new ConcurrentHashMap<>();
    private static final Map<String, List<Tuple4<Long, Integer, Event, Milestone>>> milestoneMap = new ConcurrentHashMap<>();
    private static final Map<String, List<Tuple4<Long, String, Long, Double>>> challengeMap = new ConcurrentHashMap<>();
    private static final Map<String, List<Tuple4<Long, Long, Double, String>>> raceMap = new ConcurrentHashMap<>();
    private static final Map<String, List<Tuple6<Long, Integer, String, Integer, String, Integer>>> ratingsMap = new ConcurrentHashMap<>();
    private static final Map<String, List<Tuple3<Throwable, Event, PointRule>>> pointsErrorMap = new ConcurrentHashMap<>();
    private static final Map<String, List<Tuple3<Throwable, Event, BadgeRule>>> badgesErrorMap = new ConcurrentHashMap<>();
    private static final Map<String, List<Tuple3<Throwable, Event, Milestone>>> milestoneErrorMap = new ConcurrentHashMap<>();

    private static final Map<String, List<ChallengeEvent>> challengesMap = new ConcurrentHashMap<>();


    public static void addPoint(String id, Tuple4<Long, List<? extends Event>, PointRule, Double> record) {
        pointsMap.computeIfAbsent(id, s -> new ArrayList<>()).add(record);
    }

    public static void addPointError(String id, Tuple3<Throwable, Event, PointRule> err) {
        pointsErrorMap.computeIfAbsent(id, s -> new ArrayList<>()).add(err);
    }

    public static List<Tuple4<Long, List<? extends Event>, PointRule, Double>> getPoints(String id) {
        return pointsMap.get(id);
    }

    public static List<Tuple3<Throwable, Event, PointRule>> getPointErrors(String id) {
        return pointsErrorMap.get(id);
    }

    //
    // BADGE EVENTS
    //

    public static void addBadge(String id, Tuple4<Long, List<? extends Event>, Badge, BadgeRule> record) {
        badgeMap.computeIfAbsent(id, s -> new ArrayList<>()).add(record);
    }

    public static void addBadgeError(String id, Tuple3<Throwable, Event, BadgeRule> err) {
        badgesErrorMap.computeIfAbsent(id, s -> new ArrayList<>()).add(err);
    }

    public static List<Tuple4<Long, List<? extends Event>, Badge, BadgeRule>> getBadges(String id) {
        return badgeMap.get(id);
    }

    public static List<Tuple3<Throwable, Event, BadgeRule>> getBadgeErrors(String id) {
        return badgesErrorMap.get(id);
    }

    //
    // MILESTONE EVENTS
    //

    public static void addMilestone(String id, Tuple4<Long, Integer, Event, Milestone> record) {
        milestoneMap.computeIfAbsent(id, s -> new ArrayList<>()).add(record);
    }

    public static void addMilestoneError(String id, Tuple3<Throwable, Event, Milestone> err) {
        milestoneErrorMap.computeIfAbsent(id, s -> new ArrayList<>()).add(err);
    }

    public static List<Tuple4<Long, Integer, Event, Milestone>> getMilestones(String id) {
        return milestoneMap.get(id);
    }

    public static List<Tuple3<Throwable, Event, Milestone>> getMilestoneErrors(String id) {
        return milestoneErrorMap.get(id);
    }

    //
    // RATINGS EVENTS
    //

    public static void addRating(String id, Tuple6<Long, Integer, String, Integer, String, Integer> record) {
        ratingsMap.computeIfAbsent(id, s -> new ArrayList<>()).add(record);
    }

    public static List<Tuple6<Long, Integer, String, Integer, String, Integer>> getRatings(String id) {
        return ratingsMap.get(id);
    }

    //
    // CHALLENGE EVENTS
    //

    public static void addChallenge(String id, Tuple4<Long, String, Long, Double> record) {
        challengeMap.computeIfAbsent(id, s -> new ArrayList<>()).add(record);
    }

    public static List<Tuple4<Long, String, Long, Double>> getChallenges(String id) {
        return challengeMap.get(id);
    }

    ///
    ///  RACE EVENTS
    ///

    public static void addRace(String id, Tuple4<Long, Long, Double, String> record) {
        raceMap.computeIfAbsent(id, s -> new ArrayList<>()).add(record);
    }

    public static List<Tuple4<Long, Long, Double, String>> getRaces(String id) {
        return raceMap.get(id);
    }


    //
    // CLEAR ALL
    //

    public static void clearAll(String id) {
        pointsMap.remove(id);
        pointsErrorMap.remove(id);
        milestoneMap.remove(id);
        milestoneErrorMap.remove(id);
        badgeMap.remove(id);
        badgesErrorMap.remove(id);
    }
}