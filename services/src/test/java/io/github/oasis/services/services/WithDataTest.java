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

package io.github.oasis.services.services;

import com.github.slugify.Slugify;
import io.github.oasis.model.Constants;
import io.github.oasis.model.db.DbException;
import io.github.oasis.model.defs.BadgeDef;
import io.github.oasis.model.defs.ChallengeDef;
import io.github.oasis.model.defs.MilestoneDef;
import io.github.oasis.model.defs.PointDef;
import io.github.oasis.model.defs.RatingDef;
import io.github.oasis.model.events.JsonEvent;
import io.github.oasis.model.handlers.output.BadgeModel;
import io.github.oasis.model.handlers.output.ChallengeModel;
import io.github.oasis.model.handlers.output.MilestoneModel;
import io.github.oasis.model.handlers.output.MilestoneStateModel;
import io.github.oasis.model.handlers.output.RatingModel;
import io.github.oasis.model.handlers.output.PointModel;
import io.github.oasis.services.dto.crud.TeamProfileAddDto;
import io.github.oasis.services.dto.crud.TeamScopeAddDto;
import io.github.oasis.services.dto.crud.UserProfileAddDto;
import io.github.oasis.services.model.TeamProfile;
import io.github.oasis.services.model.TeamScope;
import io.github.oasis.services.model.UserProfile;
import io.github.oasis.services.model.UserRole;
import io.github.oasis.services.model.UserTeam;
import io.github.oasis.services.services.injector.consumers.ConsumerUtils;
import io.github.oasis.services.utils.BufferedRecords;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public abstract class WithDataTest extends AbstractServiceTest {

    private static final Slugify SLUGIFY = new Slugify();

    private static List<String> ruleOrder = new ArrayList<>();
    private static List<String> milestoneOrder = new ArrayList<>();
    private static List<String> challengesOrder = new ArrayList<>();
    private static Map<String, List<String>> badgeRules = new LinkedHashMap<>();

    @Autowired
    private IProfileService ps;

    @Autowired
    private IGameDefService gameDefService;

    @Autowired
    private IProfileService profileService;

    protected Map<String, TeamScope> scopes = new HashMap<>();
    protected Map<String, TeamProfile> teams = new HashMap<>();
    protected Map<String, UserProfile> users = new HashMap<>();

    ExecutorService pool;

    private final List<BufferedRecords> buffers = new ArrayList<>();

    protected List<Long> pointRuleIds;
    List<Long> badgeIds;
    List<Long> milestoneIds;
    List<Long> challengeIds;

    protected List<Long> addPointRules(long gameId, String... rules) throws Exception {
        ruleOrder.clear();
        List<Long> ids = new ArrayList<>();
        for (String r : rules) {
            PointDef pointDef = new PointDef();
            pointDef.setName(r);
            pointDef.setEvent("so.event." + r);
            pointDef.setDisplayName(r);
            pointDef.setCondition("true");
            pointDef.setAmount(200);
            ids.add(gameDefService.addPointDef(gameId, pointDef));
        }
        ruleOrder.addAll(Arrays.asList(rules));
        pointRuleIds = new ArrayList<>(ids);
        return ids;
    }

    List<Long> addBadgeNames(long gameId, List<String>... badges) throws Exception {
        badgeIds = new ArrayList<>();
        for (List<String> b : badges) {
            Assert.assertTrue(b.size() >= 1);
            String bk = b.get(0);
            List<String> subBadges = new ArrayList<>();
            subBadges.add("");
            if (b.size() == 1) {
                badgeRules.put(bk, subBadges);
            } else {
                for (int i = 1; i < b.size(); i++) {
                    subBadges.add(b.get(i));
                }
                badgeRules.put(bk, subBadges);
            }

            BadgeDef def = new BadgeDef();
            def.setName(bk);
            def.setDisplayName(bk);
            badgeIds.add(gameDefService.addBadgeDef(gameId, def));
        }
        return badgeIds;
    }

    List<Long> addMilestoneRules(long gameId,String... names) throws Exception {
        milestoneIds = new ArrayList<>();
        Random random = new Random(System.currentTimeMillis());

        milestoneOrder = Arrays.asList(names);
        for (String name : names) {
            MilestoneDef def = new MilestoneDef();
            def.setName(SLUGIFY.slugify(name));
            def.setDisplayName(name);

            int maxLevels = 5 + random.nextInt(10);
            List<Integer> increment = Arrays.asList(500, 250, 750, 100);
            Collections.shuffle(increment);
            int incr = increment.get(0);
            Map<Integer, Object> msMap = new HashMap<>();
            for (int i = 0; i < maxLevels; i++) {
                msMap.put(i+1, incr * (i+1));
            }
            def.setLevels(msMap);

            long id = gameDefService.addMilestoneDef(gameId, def);
            milestoneIds.add(id);
        }
        return milestoneIds;
    }

    List<Long> addChallenges(long gameId, String... names) throws Exception {
        challengeIds = new ArrayList<>();
        Random random = new Random(System.currentTimeMillis());

        challengesOrder = Arrays.asList(names);
        List<UserProfile> cusers = new ArrayList<>(users.values());
        List<TeamProfile> cteams = new ArrayList<>(teams.values());
        List<TeamScope> cscopes = new ArrayList<>(scopes.values());
        for (String name : names) {
            ChallengeDef def = new ChallengeDef();
            def.setName(SLUGIFY.slugify(name));
            def.setDisplayName(name);

            int i = random.nextInt(3);
            if (i == 0) {
                UserProfile up = cusers.get(random.nextInt(cusers.size()));
                def.setForUser(up.getEmail());
            } else if (i == 1) {
                def.setForTeam(cteams.get(random.nextInt(cteams.size())).getName());
            } else if (i == 2) {
                def.setForTeamScope(cscopes.get(random.nextInt(cscopes.size())).getName());
            }

            long id = gameDefService.addChallenge(gameId, def);
            challengeIds.add(id);
        }
        return challengeIds;
    }

    void loadStateDefs(long gameId, List<String>... names) throws Exception {
        Random random = new Random(System.currentTimeMillis());

        for (List<String> attr : names) {
            String name = attr.get(0);
            RatingDef state = new RatingDef();
            state.setName(SLUGIFY.slugify(name));
            state.setDisplayName(name);
            state.setCurrency(random.nextBoolean());

            List<RatingDef.RatingState> stateList = new ArrayList<>();
            for (int i = 1; i < attr.size(); i++) {
                String st = attr.get(i);
                RatingDef.RatingState ostate = new RatingDef.RatingState();
                ostate.setName(SLUGIFY.slugify(st));
                ostate.setId(i);
                stateList.add(ostate);
            }
            state.setStates(stateList);
            state.setDefaultState(1 + random.nextInt(attr.size() - 1));

            gameDefService.addRating(gameId, state);
        }
    }

    protected void initPool(int size) {
        pool = Executors.newFixedThreadPool(size);
    }

    protected void closePool() {
        for (BufferedRecords b : buffers) {
            b.close();
        }

        if (pool != null) {
            pool.shutdown();
        }
        buffers.clear();
    }

    protected void loadUserData() throws Exception {
        scopes.clear();
        teams.clear();
        users.clear();

        List<TeamScope> teamScopes = new ArrayList<>();
        List<TeamProfile> teamProfiles = new ArrayList<>();
        List<UserProfile> userProfiles = new ArrayList<>();

        Map<Long, Long> expectedScopeCounts = new HashMap<>();
        Map<Long, Long> expectedTeamCounts = new HashMap<>();

        readLines("/dataex/scopes.csv")
                .forEach(line -> {
                    String[] parts = line.split("[,]");
                    TeamScopeAddDto dto = addTeamScope(parts[0].trim(), Long.parseLong(parts[1].trim()));
                    try {
                        teamScopes.add(ps.readTeamScope(ps.addTeamScope(dto)));
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to load team scopes!", e);
                    }
                });

        readLines("/dataex/teams.csv")
                .forEach(line -> {
                    String[] parts = line.split("[,]");
                    TeamProfileAddDto dto = addTeam(parts[0].trim());
                    Optional<TeamScope> scopeOptional = teamScopes.stream()
                            .filter(ts -> ts.getName().equals(parts[1].trim()))
                            .findFirst();
                    if (scopeOptional.isPresent()) {
                        try {
                            dto.setTeamScope(scopeOptional.get().getId());
                            TeamProfile teamProfile = ps.readTeam(ps.addTeam(dto));
                            teamProfiles.add(teamProfile);
                        } catch (Exception e) {
                            throw new RuntimeException(e.getMessage(), e);
                        }
                    } else {
                        throw new RuntimeException("No scope is found by name " + parts[1].trim());
                    }
                });

        readLines("/dataex/users.csv")
                .forEach(line -> {
                    String[] parts = line.split("[,]");
                    Optional<TeamProfile> userTeam = teamProfiles.stream()
                            .filter(t -> t.getName().equals(parts[0].trim()))
                            .findFirst();
                    if (userTeam.isPresent()) {
                        TeamProfile teamProfile = userTeam.get();

                        UserProfileAddDto dto = addUser(parts[1].trim(),
                                Boolean.parseBoolean(parts[2].trim()),
                                parts[0].trim());
                        try {
                            long u = ps.addUserProfile(dto, teamProfile.getId(), UserRole.PLAYER);
                            userProfiles.add(ps.readUserProfile(u));

                            {
                                Long count = expectedTeamCounts.computeIfAbsent(teamProfile.getId().longValue(),
                                        aLong -> 1L);   // 1 with default user
                                expectedTeamCounts.put(teamProfile.getId().longValue(), count + 1);
                            }

                        } catch (Exception e) {
                            throw new RuntimeException(e.getMessage(), e);
                        }
                    } else {
                        throw new RuntimeException("No team is found by name!" + parts[0].trim());
                    }
                });

        {
            teamProfiles.forEach(t -> {
                Long count = expectedScopeCounts.computeIfAbsent(t.getTeamScope().longValue(), aLong -> 0L);
                expectedScopeCounts.put(t.getTeamScope().longValue(),
                        count + expectedTeamCounts.get(t.getId().longValue()));

            });
        }

        teamScopes.forEach(ts -> scopes.put(ts.getName(), ts));
        teamProfiles.forEach(t -> teams.put(t.getName(), t));
        userProfiles.forEach(u -> users.put(u.getName(), u));

        Assert.assertTrue(scopes.size() > 0);
        Assert.assertTrue(teams.size() > 0);
        Assert.assertTrue(users.size() > 0);

        {
            Map<Long, Long> countMap = new HashMap<>();
            profileService.listUserCountInTeams(System.currentTimeMillis(), true)
                    .forEach(r -> countMap.put(r.getId(), r.getTotalUsers()));
            Assertions.assertThat(countMap).containsAllEntriesOf(expectedTeamCounts);
        }

        {
            Map<Long, Long> countMap = new HashMap<>();
            profileService.listUserCountInTeamScopes(System.currentTimeMillis(), true)
                    .forEach(r -> countMap.put(r.getId(), r.getTotalUsers()));
            Assertions.assertThat(countMap).containsAllEntriesOf(expectedScopeCounts);
        }

    }

    protected int loadPoints(Instant startTime, long timeRange, long gameId) throws Exception {
        Collection<UserProfile> profiles = users.values();

        BufferedRecords buffer = new BufferedRecords(this::flushPoints);
        buffers.add(buffer);
        buffer.init(pool);

        int count = 0;
        for (UserProfile profile : profiles) {
            Random random = new Random(System.currentTimeMillis());
            int eventCount = 20 + random.nextInt(20);
            List<Long> tss = orderedSeq(timeRange, eventCount, startTime.toEpochMilli());

            for (Long ts : tss) {
                UserTeam curTeam = ps.findCurrentTeamOfUser(profile.getId(), true, ts);

                int i = random.nextInt(ruleOrder.size());
                String ruleName = ruleOrder.get(i);
                long rid = pointRuleIds.get(i);


                PointModel model = new PointModel();
                model.setGameId((int) gameId);
                model.setSourceId(1);
                model.setTs(ts);
                model.setCurrency(true);
                model.setRuleName(ruleName);
                model.setEventType("so.event." + StringUtils.substringAfterLast(ruleName, "."));
                model.setUserId(profile.getId());
                model.setTeamScopeId(curTeam.getScopeId().longValue());
                model.setTeamId(curTeam.getTeamId().longValue());
                model.setAmount(Math.round(random.nextDouble() * 500 * 100) / 100.0);
                model.setRuleId(rid);

                model.setEvents(Collections.singletonList(toJsonEvent(model, gameId)));

                count++;
                Map<String, Object> data = ConsumerUtils.toPointDaoData(model);
                buffer.push(new BufferedRecords.ElementRecord(data, System.currentTimeMillis()));
            }
        }

        buffer.flushNow();
        return count;
    }

    int loadBadges(Instant startTime, long timeRange, long gameId) throws Exception {
        Collection<UserProfile> profiles = users.values();

        Assert.assertTrue(badgeRules.size() > 0);

        BufferedRecords buffer = new BufferedRecords(this::flushBadge);
        buffers.add(buffer);
        buffer.init(pool);

        ArrayList<String> badgeNameList = new ArrayList<>(badgeRules.keySet());

        int count = 0;
        for (UserProfile profile : profiles) {
            Random random = new Random(System.currentTimeMillis());
            int eventCount = 10 + random.nextInt(10);
            List<Long> tss = orderedSeq(timeRange, eventCount, startTime.toEpochMilli());

            for (Long ts : tss) {
                UserTeam curTeam = ps.findCurrentTeamOfUser(profile.getId(), true, ts);

                int i = random.nextInt(badgeNameList.size());
                long bid = badgeIds.get(i);
                String bName = badgeNameList.get(i);
                List<String> subBadges = badgeRules.get(bName);

                String sbName = subBadges.get(random.nextInt(subBadges.size()));
                if (sbName.trim().length() == 0) sbName = null;

                BadgeModel model = new BadgeModel();
                model.setGameId((int) gameId);
                model.setSourceId(1);
                model.setTs(ts);
                model.setEventType("so.event." + StringUtils.substringAfterLast(bName, "."));
                model.setUserId(profile.getId());
                model.setTeamScopeId(curTeam.getScopeId().longValue());
                model.setTeamId(curTeam.getTeamId().longValue());
                model.setBadgeId(bid);
                model.setSubBadgeId(sbName);

                model.setEvents(Collections.singletonList(toJsonEvent(model, gameId)));

                count++;
                Map<String, Object> data = ConsumerUtils.toBadgeDaoData(model);
                buffer.push(new BufferedRecords.ElementRecord(data, System.currentTimeMillis()));
            }
        }

        buffer.flushNow();
        return count;
    }

    int loadMilestones(Instant startTime, long timeRange, long gameId) throws Exception {
        Collection<UserProfile> profiles = users.values();

        Assert.assertTrue(milestoneOrder.size() > 0);

        BufferedRecords buffer = new BufferedRecords(this::flushMilestone);
        BufferedRecords stateBuffer = new BufferedRecords(this::flushMilestoneState);
        buffers.add(buffer);
        buffers.add(stateBuffer);
        buffer.init(pool);
        stateBuffer.init(pool);

        ArrayList<String> milestoneNames = new ArrayList<>(milestoneOrder);

        int count = 0;
        for (UserProfile profile : profiles) {
            long ts = System.currentTimeMillis();
            Random random = new Random(ts);
            UserTeam curTeam = ps.findCurrentTeamOfUser(profile.getId(), true, ts);

            for (int i = 0; i < milestoneIds.size(); i++) {
                long mId = milestoneIds.get(i);

                if (random.nextInt(10) % 5 == 0) continue;
                MilestoneDef def = gameDefService.readMilestoneDef(mId);
                Map<Integer, Object> levels = def.getLevels();

                int myLevel = 1 + random.nextInt(levels.size());
                int baseVal = ((Number) levels.get(myLevel)).intValue();
                int nextVal = ((Number) levels.getOrDefault(myLevel + 1, 100000)).intValue();

                MilestoneModel model = new MilestoneModel();
                model.setUserId(profile.getId());
                model.setTeamId(curTeam.getTeamId().longValue());
                model.setTeamScopeId(curTeam.getScopeId().longValue());
                model.setGameId((int)gameId);
                model.setLevel(myLevel);
                model.setMilestoneId(mId);
                model.setSourceId(1);
                model.setMaximumLevel(levels.size());
                model.setTs(System.currentTimeMillis());
                model.setEventType("so.event." + def.getName());
                model.setEvent(toJsonEvent(model, gameId));

                {
                    count++;
                    Map<String, Object> data = ConsumerUtils.toMilestoneDaoData(model);
                    buffer.push(new BufferedRecords.ElementRecord(data, System.currentTimeMillis()));
                }

                // update state
                int myCurVal = baseVal + random.nextInt(nextVal - baseVal);
                MilestoneStateModel stateModel = new MilestoneStateModel();
                stateModel.setUserId(profile.getId());
                stateModel.setMilestoneId(mId);
                stateModel.setValueInt((long) myCurVal);
                stateModel.setNextValueInt((long) nextVal);
                stateModel.setCurrBaseValueInt((long) baseVal);
                {
                    Map<String, Object> data = ConsumerUtils.toMilestoneStateDaoData(stateModel);
                    stateBuffer.push(new BufferedRecords.ElementRecord(data, System.currentTimeMillis()));
                }
            }

        }

        buffer.flushNow();
        stateBuffer.flushNow();

        Assert.assertTrue(count > 0);
        return count;
    }

    int loadChallenges(Instant startTime, long timeRange, long gameId) throws Exception {
        List<ChallengeDef> challengeDefs = gameDefService.listChallenges(gameId);

        Assert.assertTrue(challengeDefs.size() > 0);

        BufferedRecords buffer = new BufferedRecords(this::flushChallenge);
        buffers.add(buffer);
        buffer.init(pool);

        int count = 0;
        ArrayList<UserProfile> profiles = new ArrayList<>(users.values());
        List<Integer> points = Arrays.asList(100, 150, 200, 250, 500, 750, 1000);
        for (ChallengeDef challengeDef : challengeDefs) {
            Random random = new Random(System.currentTimeMillis());
            int p = random.nextInt(10);

            if (p % 4 == 0) {
                int players = 2 + random.nextInt(3);
                for (int i = 0; i < players; i++) {
                    ChallengeModel model = new ChallengeModel();
                    Collections.shuffle(points);

                    UserProfile up = profiles.get(random.nextInt(profiles.size()));
                    model.setUserId(up.getId());
                    UserTeam currentTeamOfUser = profileService.findCurrentTeamOfUser(up.getId());
                    model.setTeamId(currentTeamOfUser.getTeamId().longValue());
                    model.setTeamScopeId(currentTeamOfUser.getScopeId().longValue());
                    model.setChallengeId(challengeDef.getId());
                    model.setEventExtId(randomId());
                    model.setPoints(points.get(0) * 1.0);
                    model.setGameId((int) gameId);
                    model.setWinNo(i+1);
                    model.setSourceId(1);
                    model.setTs(System.currentTimeMillis());
                    model.setWonAt(System.currentTimeMillis());

                    Map<String, Object> data = ConsumerUtils.toChallengeDaoData(model);
                    buffer.push(new BufferedRecords.ElementRecord(data, System.currentTimeMillis()));
                }
            } else {
                ChallengeModel model = new ChallengeModel();
                UserProfile up = profiles.get(random.nextInt(profiles.size()));
                model.setUserId(up.getId());
                UserTeam currentTeamOfUser = profileService.findCurrentTeamOfUser(up.getId());
                model.setTeamId(currentTeamOfUser.getTeamId().longValue());
                model.setTeamScopeId(currentTeamOfUser.getScopeId().longValue());
                model.setChallengeId(challengeDef.getId());
                model.setEventExtId(randomId());
                model.setPoints(points.get(random.nextInt(points.size())) * 1.0);
                model.setGameId((int) gameId);
                model.setWinNo(1);
                model.setSourceId(1);
                model.setTs(System.currentTimeMillis());
                model.setWonAt(System.currentTimeMillis());

                Map<String, Object> data = ConsumerUtils.toChallengeDaoData(model);
                buffer.push(new BufferedRecords.ElementRecord(data, System.currentTimeMillis()));
            }
        }

        buffer.flushNow();
        return count;
    }

    void loadStates(long gameId) throws Exception {
        List<RatingDef> stateDefs = gameDefService.listRatings(gameId);

        Assert.assertTrue(stateDefs.size() > 0);

        BufferedRecords buffer = new BufferedRecords(this::flushState);
        buffers.add(buffer);
        buffer.init(pool);

        ArrayList<UserProfile> profiles = new ArrayList<>(users.values());
        for (UserProfile profile : profiles) {
            UserTeam team = profileService.findCurrentTeamOfUser(profile.getId());
            Random random = new Random(System.currentTimeMillis());

            for (RatingDef stateDef : stateDefs) {
                if (random.nextInt(3) % 3 == 1) continue;

                int size = stateDef.getStates().size();
                RatingDef.RatingState prevState = stateDef.getStates().get(random.nextInt(size));
                RatingDef.RatingState currState = stateDef.getStates().get(random.nextInt(size));


                RatingModel model = new RatingModel();
                model.setGameId((int) gameId);
                model.setTs(System.currentTimeMillis());
                model.setSourceId(1);
                model.setExtId(randomId());
                model.setCurrency(stateDef.isCurrency());
                model.setRatingId(stateDef.getId());
                model.setUserId(profile.getId());
                model.setPreviousState(prevState.getId());
                model.setPreviousStateName(prevState.getName());
                model.setCurrentStateName(currState.getName());
                model.setCurrentState(currState.getId());
                model.setPrevStateChangedAt(System.currentTimeMillis());
                model.setCurrentPoints(Math.round(random.nextInt(1000)) * 1.0);
                model.setCurrentValue(String.valueOf(random.nextInt(100)));
                model.setTeamId((long) team.getTeamId());
                model.setTeamScopeId((long) team.getScopeId());
                model.setEvent(toJsonEvent(model, gameId));

                Map<String, Object> data = ConsumerUtils.toStateDaoData(model);
                buffer.push(new BufferedRecords.ElementRecord(data, System.currentTimeMillis()));
            }
        }

        buffer.flushNow();
    }

    private void flushPoints(List<BufferedRecords.ElementRecord> elementRecords) {
        List<Map<String, Object>> data = elementRecords.stream().map(BufferedRecords.ElementRecord::getData)
                .collect(Collectors.toList());
        try {
            dao.executeBatchInsert("game/batch/addPoint", data);
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    private void flushBadge(List<BufferedRecords.ElementRecord> elementRecords) {
        List<Map<String, Object>> data = elementRecords.stream().map(BufferedRecords.ElementRecord::getData)
                .collect(Collectors.toList());
        try {
            dao.executeBatchInsert("game/batch/addBadge", data);
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    private void flushMilestone(List<BufferedRecords.ElementRecord> elementRecords) {
        List<Map<String, Object>> data = elementRecords.stream().map(BufferedRecords.ElementRecord::getData)
                .collect(Collectors.toList());
        try {
            dao.executeBatchInsert("game/batch/addMilestone", data);
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    private void flushMilestoneState(List<BufferedRecords.ElementRecord> elementRecords) {
        List<Map<String, Object>> data = elementRecords.stream().map(BufferedRecords.ElementRecord::getData)
                .collect(Collectors.toList());
        try {
            dao.executeBatchInsert("game/batch/updateMilestoneState", data);
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    private void flushChallenge(List<BufferedRecords.ElementRecord> elementRecords) {
        List<Map<String, Object>> data = elementRecords.stream().map(BufferedRecords.ElementRecord::getData)
                .collect(Collectors.toList());
        try {
            dao.executeBatchInsert("game/batch/addChallengeWinner", data);
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    private void flushState(List<BufferedRecords.ElementRecord> elementRecords) {
        List<Map<String, Object>> data = elementRecords.stream().map(BufferedRecords.ElementRecord::getData)
                .collect(Collectors.toList());
        try {
            for (Map<String, Object> datum : data) {
                dao.executeCommand("game/batch/updateState", datum);
            }

        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    private JsonEvent toJsonEvent(PointModel model, long gameId) {
        JsonEvent jsonEvent = new JsonEvent();
        jsonEvent.setFieldValue(Constants.FIELD_GAME_ID, gameId);
        jsonEvent.setFieldValue(Constants.FIELD_TEAM, model.getTeamId());
        jsonEvent.setFieldValue(Constants.FIELD_SCOPE, model.getTeamScopeId());
        jsonEvent.setFieldValue(Constants.FIELD_USER, model.getUserId());
        jsonEvent.setFieldValue(Constants.FIELD_TIMESTAMP, model.getTs());
        jsonEvent.setFieldValue(Constants.FIELD_ID, randomId());
        jsonEvent.setFieldValue(Constants.FIELD_EVENT_TYPE,
                "so.event." + StringUtils.substringAfterLast(model.getRuleName(), "."));
        return jsonEvent;
    }

    private JsonEvent toJsonEvent(RatingModel model, long gameId) {
        JsonEvent jsonEvent = new JsonEvent();
        jsonEvent.setFieldValue(Constants.FIELD_GAME_ID, gameId);
        jsonEvent.setFieldValue(Constants.FIELD_TEAM, model.getTeamId());
        jsonEvent.setFieldValue(Constants.FIELD_SCOPE, model.getTeamScopeId());
        jsonEvent.setFieldValue(Constants.FIELD_USER, model.getUserId());
        jsonEvent.setFieldValue(Constants.FIELD_TIMESTAMP, model.getTs());
        jsonEvent.setFieldValue(Constants.FIELD_ID, randomId());
        jsonEvent.setFieldValue(Constants.FIELD_EVENT_TYPE,
                "so.event.state." + String.valueOf(model.getRatingId()));
        return jsonEvent;
    }

    private JsonEvent toJsonEvent(BadgeModel model, long gameId) {
        JsonEvent jsonEvent = new JsonEvent();
        jsonEvent.setFieldValue(Constants.FIELD_GAME_ID, gameId);
        jsonEvent.setFieldValue(Constants.FIELD_TEAM, model.getTeamId());
        jsonEvent.setFieldValue(Constants.FIELD_SCOPE, model.getTeamScopeId());
        jsonEvent.setFieldValue(Constants.FIELD_USER, model.getUserId());
        jsonEvent.setFieldValue(Constants.FIELD_TIMESTAMP, model.getTs());
        jsonEvent.setFieldValue(Constants.FIELD_ID, randomId());
        jsonEvent.setFieldValue(Constants.FIELD_EVENT_TYPE,
                "so.event." + StringUtils.substringAfterLast(String.valueOf(model.getBadgeId()), "."));
        return jsonEvent;
    }

    private JsonEvent toJsonEvent(MilestoneModel model, long gameId) {
        JsonEvent jsonEvent = new JsonEvent();
        jsonEvent.setFieldValue(Constants.FIELD_GAME_ID, gameId);
        jsonEvent.setFieldValue(Constants.FIELD_TEAM, model.getTeamId());
        jsonEvent.setFieldValue(Constants.FIELD_SCOPE, model.getTeamScopeId());
        jsonEvent.setFieldValue(Constants.FIELD_USER, model.getUserId());
        jsonEvent.setFieldValue(Constants.FIELD_TIMESTAMP, model.getTs());
        jsonEvent.setFieldValue(Constants.FIELD_ID, randomId());
        jsonEvent.setFieldValue(Constants.FIELD_EVENT_TYPE, model.getEventType());
        return jsonEvent;
    }

    private List<Long> orderedSeq(long range, int n, long offset) {
        Random random = new Random(System.currentTimeMillis());
        return random.longs(n, 0, range)
                .map(operand -> offset + operand)
                .boxed()
                .collect(Collectors.toList());
    }

    private String randomId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static List<String> readLines(String resPath) {
        try (InputStream inputStream = LeaderboardTest.class.getResourceAsStream(resPath)) {
            return IOUtils.readLines(inputStream, StandardCharsets.UTF_8).stream()
                    .filter(l -> !l.trim().isEmpty() && !l.trim().startsWith("#"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new LinkedList<>();
    }

    private TeamScopeAddDto addTeamScope(String name, long extId) {
        TeamScopeAddDto s = new TeamScopeAddDto();
        s.setName(SLUGIFY.slugify(name));
        s.setDisplayName(name);
        s.setAutoScope(false);
        s.setExtId(extId);
        return s;
    }

    private TeamProfileAddDto addTeam(String name) {
        TeamProfileAddDto t = new TeamProfileAddDto();
        t.setName(SLUGIFY.slugify(name));
        t.setAutoTeam(false);
        t.setAvatarId(String.format("images/t/%s.jpg", SLUGIFY.slugify(name)));
        return t;
    }

    private UserProfileAddDto addUser(String name, boolean male, String team) {
        UserProfileAddDto u = new UserProfileAddDto();
        String dn = name.split("[ ]+")[0];
        u.setName(SLUGIFY.slugify(name));
        u.setNickName(name);
        u.setMale(male);
        u.setEmail(SLUGIFY.slugify(dn) + "@" + team + ".com");
        u.setAvatarId(String.format("images/u/%s.jpg", SLUGIFY.slugify(dn)));
        return u;
    }
}