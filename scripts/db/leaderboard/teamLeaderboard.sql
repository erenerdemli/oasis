<if(hasTeam||hasUser||isTopN||isBottomN||onlyFinalTops)>
SELECT
    *
FROM
(
<endif>

    SELECT
        tbl.user_id AS userId,
        COALESCE(oau.nickname, oau.user_name, oau.email) AS userName,
        tbl.team_id AS teamId,
        oat.name AS teamName,
        tbl.team_scope_id AS teamScopeId,
        COALESCE(oats.display_name, oats.name) AS teamScopeName,

        tbl.totalPoints AS totalPoints,
        tbl.totalCount AS totalCount,
        (RANK() over (PARTITION BY tbl.team_id ORDER BY tbl.totalPoints DESC, tbl.totalCount ASC)) AS 'rankInTeam',
        (RANK() over (PARTITION BY tbl.team_scope_id ORDER BY tbl.totalPoints DESC, tbl.totalCount ASC)) AS 'rankInTeamScope',
        (LAG(tbl.totalPoints) over (PARTITION BY tbl.team_id ORDER BY tbl.totalPoints DESC, tbl.totalCount ASC)) AS 'nextTeamRankValue',
        (LAG(tbl.totalPoints) over (PARTITION BY tbl.team_scope_id ORDER BY tbl.totalPoints DESC, tbl.totalCount ASC)) AS 'nextTeamScopeRankValue',
        UNIX_TIMESTAMP(NOW()) * 1000 AS calculatedTime
    FROM
    (
       <if(hasStates)>
        SELECT
          user_id,
          team_scope_id,
          team_id,
          SUM(totalCount) AS totalCount,
          ROUND(<aggType>(totalPoints), 2) AS totalPoints
        FROM (
       <endif>

            SELECT
                user_id,
                team_scope_id,
                team_id,
                COUNT(points) AS totalCount,
                ROUND(<aggType>(points), 2) AS totalPoints
            FROM OA_POINT
            WHERE
                is_active = 1
                AND
                team_scope_id = :teamScopeId
                <if(hasTimeRange)>
                AND ts >= :rangeStart
                AND ts \< :rangeEnd
                <endif>

                <if(hasInclusions)>
                AND point_id IN \<ruleIds>
                <endif>
                <if(hasExclusions)>
                AND point_id NOT IN \<excludeRuleIds>
                <endif>
            GROUP BY
                team_id,
                team_scope_id,
                user_id

       <if(hasStates)>
        UNION ALL
            SELECT
                user_id,
                team_scope_id,
                team_id,
                COUNT(current_points) AS totalCount,
                ROUND(<aggType>(current_points), 2) AS totalPoints
            FROM OA_STATE
            WHERE
                is_active = 1
                AND
                team_scope_id = :teamScopeId
                <if(hasTimeRange)>
                AND changed_at >= :rangeStart
                AND changed_at \< :rangeEnd
                <endif>
            GROUP BY
                user_id, team_scope_id, team_id
        ) grpPoints
        GROUP BY grpPoints.user_id, grpPoints.team_scope_id, grpPoints.team_id
       <endif>

    ) tbl
    INNER JOIN OA_USER oau ON oau.user_id = tbl.user_id
    INNER JOIN OA_TEAM oat ON oat.team_id = tbl.team_id
    INNER JOIN OA_TEAM_SCOPE oats ON oats.scope_id = tbl.team_scope_id

<if(hasTeam||hasUser||isTopN||isBottomN)>
) rankTbl

WHERE
    1 = 1
<if(hasTeam)>
    AND rankTbl.teamId = :teamId
<endif>
<if(hasUser)>
    AND rankTbl.userId = :userId
<endif>

<if(onlyFinalTops)>
    <if(hasTeam)>
    AND rankTbl.rankInTeam \<= :topThreshold
    <else>
    AND rankTbl.rankInTeamScope \<= :topThreshold
    <endif>
<endif>

<if(isTopN)>
    <if(hasTeam)>
    ORDER BY rankTbl.rankTeam ASC
    <else>
    ORDER BY rankTbl.rankTeamScope ASC
    <endif>
LIMIT :topN
<endif>

<if(isBottomN)>
    <if(hasTeam)>
    ORDER BY rankTbl.rankTeam DESC
    <else>
    ORDER BY rankTbl.rankTeamScope DESC
    <endif>
LIMIT :bottomN
<endif>

<endif>
