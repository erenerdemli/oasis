package io.github.isuru.oasis.services.api;

import io.github.isuru.oasis.services.model.TeamProfile;
import io.github.isuru.oasis.services.model.TeamScope;
import io.github.isuru.oasis.services.model.UserProfile;
import io.github.isuru.oasis.services.model.UserTeam;

import java.util.List;

public interface IProfileService {

    long addUserProfile(UserProfile profile) throws Exception;
    UserProfile readUserProfile(long userId) throws Exception;
    UserProfile readUserProfile(String email) throws Exception;
    UserProfile readUserProfileByExtId(long extUserId) throws Exception;
    boolean editUserProfile(long userId, UserProfile profile) throws Exception;
    boolean deleteUserProfile(long userId) throws Exception;
    List<UserProfile> findUser(String email, String name) throws Exception;

    List<UserProfile> listUsers(long teamId, long offset, long size) throws Exception;

    long addTeam(TeamProfile teamProfile) throws Exception;
    TeamProfile readTeam(long teamId) throws Exception;
    boolean editTeam(long teamId, TeamProfile teamProfile) throws Exception;
    List<TeamProfile> listTeams(long scopeId) throws Exception;

    long addTeamScope(TeamScope teamScope) throws Exception;
    TeamScope readTeamScope(long scopeId) throws Exception;
    TeamScope readTeamScope(String scopeName) throws Exception;
    List<TeamScope> listTeamScopes() throws Exception;
    boolean editTeamScope(long scopeId, TeamScope scope) throws Exception;

    boolean addUserToTeam(long userId, long teamId, int roleId) throws Exception;
    UserTeam findCurrentTeamOfUser(long userId) throws Exception;
    TeamProfile findTeamByName(String name) throws Exception;

    boolean logoutUser(long userId, long ts) throws Exception;

    boolean changeUserHero(long userId, long newHeroId) throws Exception;
}
