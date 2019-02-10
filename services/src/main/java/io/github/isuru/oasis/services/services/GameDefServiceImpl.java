package io.github.isuru.oasis.services.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.isuru.oasis.model.db.DbException;
import io.github.isuru.oasis.model.db.IOasisDao;
import io.github.isuru.oasis.model.defs.BadgeDef;
import io.github.isuru.oasis.model.defs.ChallengeDef;
import io.github.isuru.oasis.model.defs.Converters;
import io.github.isuru.oasis.model.defs.DefWrapper;
import io.github.isuru.oasis.model.defs.GameDef;
import io.github.isuru.oasis.model.defs.KpiDef;
import io.github.isuru.oasis.model.defs.LeaderboardDef;
import io.github.isuru.oasis.model.defs.MilestoneDef;
import io.github.isuru.oasis.model.defs.OasisDefinition;
import io.github.isuru.oasis.model.defs.PointDef;
import io.github.isuru.oasis.model.defs.RaceDef;
import io.github.isuru.oasis.model.defs.StateDef;
import io.github.isuru.oasis.services.Bootstrapping;
import io.github.isuru.oasis.services.dto.defs.GameOptionsDto;
import io.github.isuru.oasis.services.exception.InputValidationException;
import io.github.isuru.oasis.services.model.*;
import io.github.isuru.oasis.services.utils.Checks;
import io.github.isuru.oasis.services.utils.Commons;
import io.github.isuru.oasis.services.utils.Maps;
import io.github.isuru.oasis.services.utils.RUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author iweerarathna
 */
@Service("gameDefService")
public class GameDefServiceImpl implements IGameDefService {

    @Autowired
    private IOasisDao dao;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private IProfileService profileService;

    @Override
    public long addAttribute(long gameId, FeatureAttr featureAttr) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(featureAttr.getName(), "name");
        Checks.nonNullOrEmpty(featureAttr.getDisplayName(), "displayName");
        Checks.greaterThanZero(featureAttr.getPriority(), "priority");

        GameDef gameDef = readGame(gameId);

        Map<String, Object> data = Maps.create()
                .put("name", featureAttr.getName())
                .put("displayName", featureAttr.getDisplayName())
                .put("priority", featureAttr.getPriority())
                .put("gameId", gameDef.getId())
                .build();

        return dao.executeInsert(Q.DEF.ADD_ATTRIBUTE, data, "id");
    }

    @Override
    public List<FeatureAttr> listAttributes(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        return ServiceUtils.toList(dao.executeQuery(
                Q.DEF.LIST_ATTRIBUTE, null, FeatureAttr.class));
    }

    @Override
    public long createGame(GameDef gameDef, GameOptionsDto optionsDto) throws Exception {
        Checks.nonNull(gameDef, "gameDef");
        Checks.nonNull(optionsDto, "gameOptions");
        Checks.nonNullOrEmpty(gameDef.getName(), "game.name");

        if (listGames().stream()
                .anyMatch(g -> g.getName().equalsIgnoreCase(gameDef.getName()))) {
            throw new InputValidationException("There is already a game with same name!");
        }

        DefWrapper wrapper = new DefWrapper();
        wrapper.setKind(OasisDefinition.GAME.getTypeId());
        wrapper.setName(gameDef.getName());
        wrapper.setDisplayName(gameDef.getDisplayName());
        wrapper.setContent(RUtils.toStr(gameDef, mapper));

        long gameId = dao.getDefinitionDao().addDefinition(wrapper);

        Bootstrapping.initGame(this, gameId, optionsDto);
        return gameId;
    }

    @Override
    public GameDef readGame(long gameId) throws DbException, InputValidationException {
        Checks.greaterThanZero(gameId, "gameId");

        DefWrapper defWrapper = dao.getDefinitionDao().readDefinition(gameId);
        if (defWrapper == null) {
            throw new InputValidationException("No game definition is found by id " + gameId + "!");
        }
        GameDef def = RUtils.toObj(defWrapper.getContent(), GameDef.class, mapper);
        def.setId(gameId);
        return def;
    }

    @Override
    public List<GameDef> listGames() throws DbException {
        List<DefWrapper> wrappers = dao.getDefinitionDao().listDefinitions(OasisDefinition.GAME.getTypeId());
        List<GameDef> gameDefs = new LinkedList<>();
        for (DefWrapper wrp : wrappers) {
            GameDef gameDef = RUtils.toObj(wrp.getContent(), GameDef.class, mapper);
            gameDef.setId(wrp.getId());
            gameDefs.add(gameDef);
        }
        return gameDefs;
    }

    @Override
    public boolean disableGame(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        dao.executeCommand("def/disableAllGameDefs", Maps.create("gameId", gameId));
        return dao.getDefinitionDao().disableDefinition(gameId);
    }

    @Override
    public boolean addGameConstants(long gameId, Map<String, Object> gameConstants) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(gameConstants, "gameConstants");

        DefWrapper wrp = dao.getDefinitionDao().readDefinition(gameId);

        GameDef gameDef = RUtils.toObj(wrp.getContent(), GameDef.class, mapper);
        if (gameDef.getConstants() == null) {
            gameDef.setConstants(gameConstants);
        } else {
            Map<String, Object> tmp = new HashMap<>(gameDef.getConstants());
            tmp.putAll(gameConstants);
            gameDef.setConstants(tmp);
        }
        wrp.setContent(RUtils.toStr(gameDef, mapper));

        return dao.getDefinitionDao().editDefinition(wrp.getId(), wrp) > 0;
    }

    @Override
    public boolean removeGameConstants(long gameId, List<String> gameConstants) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(gameConstants, "gameConstantNames");

        DefWrapper wrp = dao.getDefinitionDao().readDefinition(gameId);

        GameDef gameDef = RUtils.toObj(wrp.getContent(), GameDef.class, mapper);
        if (!Commons.isNullOrEmpty(gameDef.getConstants())) {
            int deleted = 0;
            for (String k : gameConstants) {
                if (gameDef.getConstants().remove(k) != null) {
                    deleted++;
                }
            }

            if (deleted == 0) {
                return false;
            }
        } else {
            return false;
        }
        wrp.setContent(RUtils.toStr(gameDef, mapper));

        return dao.getDefinitionDao().editDefinition(wrp.getId(), wrp) > 0;
    }

    @Override
    public long addKpiCalculation(long gameId, KpiDef kpi) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(kpi.getName(), "kpiName");
        Checks.nonNullOrEmpty(kpi.getField(), "field");
        Checks.nonNullOrEmpty(kpi.getExpression(), "expression");

        DefWrapper wrapper = new DefWrapper();
        wrapper.setKind(OasisDefinition.KPI.getTypeId());
        wrapper.setName(kpi.getName());
        wrapper.setDisplayName(kpi.getDisplayName());
        wrapper.setContent(RUtils.toStr(kpi, mapper));
        wrapper.setGameId(gameId);

        return dao.getDefinitionDao().addDefinition(wrapper);
    }

    @Override
    public List<KpiDef> listKpiCalculations() throws Exception {
        return dao.getDefinitionDao().listDefinitions(OasisDefinition.KPI.getTypeId())
                .stream()
                .map(this::wrapperToKpi)
                .collect(Collectors.toList());
    }

    @Override
    public List<KpiDef> listKpiCalculations(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        return dao.getDefinitionDao().listDefinitionsOfGame(gameId, OasisDefinition.KPI.getTypeId())
                .stream()
                .map(this::wrapperToKpi)
                .collect(Collectors.toList());
    }

    @Override
    public KpiDef readKpiCalculation(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return wrapperToKpi(dao.getDefinitionDao().readDefinition(id));
    }

    @Override
    public boolean disableKpiCalculation(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return dao.getDefinitionDao().disableDefinition(id);
    }

    @Override
    public long addBadgeDef(long gameId, BadgeDef badge) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(badge.getName(), "name");
        Checks.nonNullOrEmpty(badge.getDisplayName(), "displayName");

        DefWrapper wrapper = new DefWrapper();
        wrapper.setKind(OasisDefinition.BADGE.getTypeId());
        wrapper.setName(badge.getName());
        wrapper.setDisplayName(badge.getDisplayName());
        wrapper.setContent(RUtils.toStr(badge, mapper));
        wrapper.setGameId(gameId);

        long id = dao.getDefinitionDao().addDefinition(wrapper);

        // add badge attributes
        List<Map<String, Object>> attrs = new ArrayList<>();
        if (ServiceUtils.isValid(badge.getAttribute())) {
            attrs.add(Maps.create()
                    .put("defId", id)
                    .put("defSubId", "")
                    .put("attributeId", badge.getAttribute()).build());
        }

        if (!Commons.isNullOrEmpty(badge.getSubBadges())) {
            for (BadgeDef.SubBadgeDef subBadgeDef : badge.getSubBadges()) {
                if (ServiceUtils.isValid(subBadgeDef.getAttribute())) {
                    attrs.add(Maps.create()
                            .put("defId", id)
                            .put("defSubId", subBadgeDef.getName())
                            .put("attributeId", subBadgeDef.getAttribute()).build());
                }
            }
        }

        if (!attrs.isEmpty()) {
            dao.executeBatchInsert(Q.DEF.ADD_DEF_ATTRIBUTE, attrs);
        }
        return id;
    }

    @Override
    public List<BadgeDef> listBadgeDefs() throws Exception {
        List<BadgeDef> badges = dao.getDefinitionDao().listDefinitions(OasisDefinition.BADGE.getTypeId())
                .stream()
                .map(this::wrapperToBadge)
                .collect(Collectors.toList());

        Iterable<DefinitionAttr> defAttrs = dao.executeQuery(Q.DEF.LIST_ALL_DEF_ATTRIBUTE,
                null,
                DefinitionAttr.class);
        matchAttrsWithBadges(defAttrs, badges);
        return badges;
    }

    @Override
    public List<BadgeDef> listBadgeDefs(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        List<BadgeDef> badges = dao.getDefinitionDao().listDefinitionsOfGame(gameId, OasisDefinition.BADGE.getTypeId())
                .stream()
                .map(this::wrapperToBadge)
                .collect(Collectors.toList());

        Iterable<DefinitionAttr> defAttrs = dao.executeQuery(Q.DEF.LIST_DEF_ATTRIBUTE_GAME,
                Maps.create("gameId", gameId), DefinitionAttr.class);
        matchAttrsWithBadges(defAttrs, badges);
        return badges;
    }

    @Override
    public BadgeDef readBadgeDef(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        BadgeDef badgeDef = wrapperToBadge(dao.getDefinitionDao().readDefinition(id));

        // read feature attr
        Iterable<DefinitionAttr> definitionAttrs = dao.executeQuery(Q.DEF.LIST_DEF_ATTRIBUTE,
                Maps.create("defId", id),
                DefinitionAttr.class);
        matchAttrsWithBadges(definitionAttrs, Collections.singletonList(badgeDef));
        return badgeDef;
    }

    @Override
    public boolean disableBadgeDef(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        boolean success = dao.getDefinitionDao().disableDefinition(id);
        if (success) {
            // delete all attrs
            dao.executeCommand(Q.DEF.DISABLE_DEF_ATTRIBUTES, Maps.create("defId", id));
        }
        return success;
    }

    @Override
    public List<PointDef> listPointDefs(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        return dao.getDefinitionDao().listDefinitionsOfGame(gameId, OasisDefinition.POINT.getTypeId())
                .stream()
                .map(this::wrapperToPoint)
                .collect(Collectors.toList());
    }

    @Override
    public long addPointDef(long gameId, PointDef pointRule) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(pointRule.getName(), "name");
        Checks.nonNullOrEmpty(pointRule.getDisplayName(), "displayName");
        Checks.havingBoth(!Commons.isNullOrEmpty(pointRule.getCondition()),
            !Commons.isNullOrEmpty(pointRule.getConditionClass()), "condition", "conditionClass");

        DefWrapper wrapper = new DefWrapper();
        wrapper.setKind(OasisDefinition.POINT.getTypeId());
        wrapper.setName(pointRule.getName());
        wrapper.setDisplayName(pointRule.getDisplayName());
        wrapper.setContent(RUtils.toStr(pointRule, mapper));
        wrapper.setGameId(gameId);

        return dao.getDefinitionDao().addDefinition(wrapper);
    }

    @Override
    public PointDef readPointDef(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return wrapperToPoint(dao.getDefinitionDao().readDefinition(id));
    }

    @Override
    public boolean disablePointDef(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return dao.getDefinitionDao().disableDefinition(id);
    }

    @Override
    public long addMilestoneDef(long gameId, MilestoneDef milestone) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(milestone.getName(), "name");
        Checks.nonNullOrEmpty(milestone.getDisplayName(), "displayName");

        DefWrapper wrapper = new DefWrapper();
        wrapper.setKind(OasisDefinition.MILESTONE.getTypeId());
        wrapper.setName(milestone.getName());
        wrapper.setDisplayName(milestone.getDisplayName());
        wrapper.setContent(RUtils.toStr(milestone, mapper));
        wrapper.setGameId(gameId);

        return dao.getDefinitionDao().addDefinition(wrapper);
    }

    @Override
    public List<MilestoneDef> listMilestoneDefs() throws Exception {
        return dao.getDefinitionDao().listDefinitions(OasisDefinition.MILESTONE.getTypeId())
                .stream()
                .map(this::wrapperToMilestone)
                .collect(Collectors.toList());
    }

    @Override
    public List<MilestoneDef> listMilestoneDefs(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        return dao.getDefinitionDao().listDefinitionsOfGame(gameId, OasisDefinition.MILESTONE.getTypeId())
                .stream()
                .map(this::wrapperToMilestone)
                .collect(Collectors.toList());
    }

    @Override
    public MilestoneDef readMilestoneDef(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return wrapperToMilestone(dao.getDefinitionDao().readDefinition(id));
    }

    @Override
    public boolean disableMilestoneDef(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return dao.getDefinitionDao().disableDefinition(id);
    }

    @Override
    public long addLeaderboardDef(long gameId, LeaderboardDef leaderboardDef) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(leaderboardDef.getName(), "name");
        Checks.nonNullOrEmpty(leaderboardDef.getDisplayName(), "displayName");
        Checks.nonNullOrEmpty(leaderboardDef.getOrderBy(), "orderBy");
        Checks.isOneOf(leaderboardDef.getOrderBy(), LeaderboardDef.ORDER_BY_ALLOWED, "orderBy");
        Checks.validate(Commons.isNullOrEmpty(leaderboardDef.getRuleIds()) ||
                Commons.isNullOrEmpty(leaderboardDef.getExcludeRuleIds()),
                "Both inclusion and exclusion rule ids cannot exist together!");

        DefWrapper wrapper = new DefWrapper();
        wrapper.setKind(OasisDefinition.LEADERBOARD.getTypeId());
        wrapper.setName(leaderboardDef.getName());
        wrapper.setDisplayName(leaderboardDef.getDisplayName());
        wrapper.setContent(RUtils.toStr(leaderboardDef, mapper));
        wrapper.setGameId(gameId);

        return dao.getDefinitionDao().addDefinition(wrapper);
    }

    @Override
    public List<LeaderboardDef> listLeaderboardDefs(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        return dao.getDefinitionDao().listDefinitionsOfGame(gameId, OasisDefinition.LEADERBOARD.getTypeId())
                .stream()
                .map(this::wrapperToLeaderboard)
                .collect(Collectors.toList());
    }

    @Override
    public List<LeaderboardDef> listLeaderboardDefs() throws Exception {
        return dao.getDefinitionDao().listDefinitions(OasisDefinition.LEADERBOARD.getTypeId())
                .stream()
                .map(this::wrapperToLeaderboard)
                .collect(Collectors.toList());
    }

    @Override
    public LeaderboardDef readLeaderboardDef(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return wrapperToLeaderboard(dao.getDefinitionDao().readDefinition(id));
    }

    @Override
    public boolean disableLeaderboardDef(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return dao.getDefinitionDao().disableDefinition(id);
    }

    @Override
    public long addChallenge(long gameId, ChallengeDef challengeDef) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(challengeDef.getName(), "name");
        Checks.nonNullOrEmpty(challengeDef.getDisplayName(), "displayName");

        // map user and team ids, if specified
        if (!Commons.isNullOrEmpty(challengeDef.getForUser())) {
            UserProfile profile = profileService.readUserProfile(challengeDef.getForUser());
            if (profile == null) {
                throw new InputValidationException("No user is found by email address " + challengeDef.getForUser() + "!");
            }
            challengeDef.setForUserId(profile.getId());
        }
        if (!Commons.isNullOrEmpty(challengeDef.getForTeam())) {
            TeamProfile teamByName = profileService.findTeamByName(challengeDef.getForTeam());
            if (teamByName == null) {
                throw new InputValidationException("No team is found by name '" + challengeDef.getForTeam() + "'!");
            }
            challengeDef.setForTeamId(teamByName.getId().longValue());
        }
        if (!Commons.isNullOrEmpty(challengeDef.getForTeamScope())) {
            TeamScope scope = profileService.readTeamScope(challengeDef.getForTeamScope());
            if (scope == null) {
                throw new InputValidationException("No team scope is found by name '"
                        + challengeDef.getForTeamScope() + "'!");
            }
            challengeDef.setForTeamId(scope.getId().longValue());
        }

        DefWrapper wrapper = new DefWrapper();
        wrapper.setKind(OasisDefinition.CHALLENGE.getTypeId());
        wrapper.setName(challengeDef.getName());
        wrapper.setDisplayName(challengeDef.getDisplayName());
        wrapper.setContent(RUtils.toStr(challengeDef, mapper));
        wrapper.setGameId(gameId);

        return dao.getDefinitionDao().addDefinition(wrapper);
    }

    @Override
    public ChallengeDef readChallenge(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return wrapperToChallenge(dao.getDefinitionDao().readDefinition(id));
    }

    @Override
    public List<ChallengeDef> listChallenges(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        return dao.getDefinitionDao().listDefinitionsOfGame(gameId, OasisDefinition.CHALLENGE.getTypeId())
                .stream()
                .map(this::wrapperToChallenge)
                .collect(Collectors.toList());
    }

    @Override
    public boolean disableChallenge(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return dao.getDefinitionDao().disableDefinition(id);
    }

    @Override
    public long addStatePlay(long gameId, StateDef stateDef) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(stateDef.getName(), "name");
        Checks.nonNullOrEmpty(stateDef.getDisplayName(), "displayName");

        DefWrapper wrapper = new DefWrapper();
        wrapper.setKind(OasisDefinition.STATE.getTypeId());
        wrapper.setName(stateDef.getName());
        wrapper.setDisplayName(stateDef.getDisplayName());
        wrapper.setContent(RUtils.toStr(stateDef, mapper));
        wrapper.setGameId(gameId);

        return dao.getDefinitionDao().addDefinition(wrapper);
    }

    @Override
    public StateDef readStatePlay(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return wrapperToStatePlay(dao.getDefinitionDao().readDefinition(id));
    }

    @Override
    public List<StateDef> listStatePlays(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        return dao.getDefinitionDao().listDefinitionsOfGame(gameId, OasisDefinition.STATE.getTypeId())
                .stream()
                .map(this::wrapperToStatePlay)
                .collect(Collectors.toList());
    }

    @Override
    public boolean disableStatePlay(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return dao.getDefinitionDao().disableDefinition(id);
    }

    @Override
    public long addRace(long gameId, RaceDef raceDef) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");
        Checks.nonNullOrEmpty(raceDef.getName(), "name");
        Checks.nonNullOrEmpty(raceDef.getDisplayName(), "displayName");
        Checks.greaterThanZero(raceDef.getLeaderboardId(), "leaderboardId");
        Checks.havingBoth(ServiceUtils.isValid(raceDef.getTop()),
                ServiceUtils.isValid(raceDef.getMinPointThreshold()), "top", "minPointThreshold");
        Checks.onlyOneOf(!Commons.isNullOrEmpty(raceDef.getRankPointsExpression()),
                !Commons.isNullOrEmpty(raceDef.getRankPoints()), "rankPointExpression", "rankPointMap");
        Checks.nonNullOrEmpty(raceDef.getFromScope(), "fromScope");
        Checks.isOneOf(raceDef.getFromScope().toUpperCase(), RaceDef.FROM_SCOPES, "fromScope");
        Checks.nonNullOrEmpty(raceDef.getTimeWindow(), "timeWindow");
        Checks.isOneOf(raceDef.getTimeWindow().toUpperCase(), RaceDef.TIME_WINDOWS, "timeWindow");

        if (listLeaderboardDefs(gameId).stream().noneMatch(l -> l.getId() == raceDef.getLeaderboardId())) {
            throw new InputValidationException("Leaderboard does not exist by id #" + raceDef.getLeaderboardId() + "!");
        }

        DefWrapper wrapper = new DefWrapper();
        wrapper.setKind(OasisDefinition.RACE.getTypeId());
        wrapper.setName(raceDef.getName());
        wrapper.setDisplayName(raceDef.getDisplayName());
        wrapper.setContent(RUtils.toStr(raceDef, mapper));
        wrapper.setGameId(gameId);

        return dao.getDefinitionDao().addDefinition(wrapper);
    }

    @Override
    public RaceDef readRace(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return wrapperToRace(dao.getDefinitionDao().readDefinition(id));
    }

    @Override
    public List<RaceDef> listRaces(long gameId) throws Exception {
        Checks.greaterThanZero(gameId, "gameId");

        return dao.getDefinitionDao().listDefinitionsOfGame(gameId, OasisDefinition.RACE.getTypeId())
                .stream()
                .map(this::wrapperToRace)
                .collect(Collectors.toList());
    }

    @Override
    public boolean disableRace(long id) throws Exception {
        Checks.greaterThanZero(id, "id");

        return dao.getDefinitionDao().disableDefinition(id);
    }

    private ChallengeDef wrapperToChallenge(DefWrapper wrapper) {
        if (wrapper == null) {
            return null;
        }
        return Converters.toChallengeDef(wrapper,
                wrp -> RUtils.toObj(wrp.getContent(), ChallengeDef.class, mapper));
    }

    private StateDef wrapperToStatePlay(DefWrapper wrapper) {
        return Converters.toStateDef(wrapper,
                wrp -> RUtils.toObj(wrp.getContent(), StateDef.class, mapper));
    }

    private RaceDef wrapperToRace(DefWrapper wrapper) {
        return Converters.toRaceDef(wrapper,
                wrp -> RUtils.toObj(wrp.getContent(), RaceDef.class, mapper));
    }

    private BadgeDef wrapperToBadge(DefWrapper wrapper) {
        return Converters.toBadgeDef(wrapper,
                wrp -> RUtils.toObj(wrp.getContent(), BadgeDef.class, mapper));
    }

    private KpiDef wrapperToKpi(DefWrapper wrapper) {
        return Converters.toKpiDef(wrapper,
                wrp -> RUtils.toObj(wrp.getContent(), KpiDef.class, mapper));
    }

    private PointDef wrapperToPoint(DefWrapper wrapper) {
        return Converters.toPointDef(wrapper,
                wrp -> RUtils.toObj(wrp.getContent(), PointDef.class, mapper));
    }

    private MilestoneDef wrapperToMilestone(DefWrapper wrapper) {
        return Converters.toMilestoneDef(wrapper,
                wrp -> RUtils.toObj(wrp.getContent(), MilestoneDef.class, mapper));
    }

    private LeaderboardDef wrapperToLeaderboard(DefWrapper wrapper) {
        if (wrapper == null) {
            return null;
        }
        return Converters.toLeaderboardDef(wrapper,
                wrp -> RUtils.toObj(wrp.getContent(), LeaderboardDef.class, mapper));
    }

    private void matchAttrsWithBadges(Iterable<DefinitionAttr> definitionAttrs,
                                      Collection<BadgeDef> badgeDefs) {
        for (DefinitionAttr attr : definitionAttrs) {
            for (BadgeDef badgeDef : badgeDefs) {
                if (badgeDef.getId().equals(attr.getDefId())) {
                    if (Commons.isNullOrEmpty(attr.getDefSubId())) {
                        badgeDef.setAttribute(attr.getAttrId());
                    } else {
                        badgeDef.findSubBadge(attr.getDefSubId())
                                .ifPresent(subBadgeDef -> subBadgeDef.setAttribute(attr.getAttrId()));
                    }
                }
            }
        }
    }

}
