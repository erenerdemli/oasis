package io.github.isuru.oasis.services.services;

import io.github.isuru.oasis.model.defs.LeaderboardType;
import io.github.isuru.oasis.services.dto.game.UserRankRecordDto;
import io.github.isuru.oasis.services.dto.game.UserRankingsInRangeDto;
import io.github.isuru.oasis.services.dto.stats.*;
import io.github.isuru.oasis.services.model.PurchasedItem;
import io.github.isuru.oasis.model.defs.ScopingType;

import java.util.List;

/**
 * @author iweerarathna
 */
public interface IStatService {

    PointBreakdownResDto getPointBreakdownList(PointBreakdownReqDto request) throws Exception;
    BadgeBreakdownResDto getBadgeBreakdownList(BadgeBreakdownReqDto request) throws Exception;

    UserStatDto readUserGameStats(long userId, long since) throws Exception;
    List<PurchasedItem> readUserPurchasedItems(long userId, long since) throws Exception;
    List<UserBadgeStatDto> readUserBadgesSummary(long userId, UserBadgeStatReq req) throws Exception;
    List<UserMilestoneStatDto> readUserMilestones(long userId) throws Exception;
    UserRankingsInRangeDto readUserTeamRankings(long userId) throws Exception;
    List<UserRankRecordDto> readMyLeaderboardRankings(long gameId, long userId, ScopingType scopingType,
                                                      LeaderboardType rangeType) throws Exception;
    List<TeamHistoryRecordDto> readUserTeamHistoryStat(long userId) throws Exception;
    List<UserStateStatDto> readUserStateStats(long userId, long teamId) throws Exception;

    ChallengeInfoDto readChallengeStats(long challengeId) throws Exception;
    void readUserGameTimeline(long userId);

}
