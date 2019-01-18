package io.github.isuru.oasis.services.services;

import com.github.slugify.Slugify;
import io.github.isuru.oasis.model.DefaultEntities;
import io.github.isuru.oasis.model.db.DbException;
import io.github.isuru.oasis.services.exception.InputValidationException;
import io.github.isuru.oasis.services.model.*;
import io.github.isuru.oasis.services.utils.Maps;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class ProfileServiceTest extends AbstractServiceTest {

    private static final Slugify SLUGIFY = new Slugify();

    private static final long ONE_DAY = 3600L * 24 * 1000;

    @Autowired
    private IProfileService ps;

    @Before
    public void verify() throws Exception {
        // drop schema
        resetSchema();

        List<TeamScope> teamScopes = ps.listTeamScopes();
        Assertions.assertThat(teamScopes)
                .isNotNull()
                .hasSize(1)
                .allMatch(TeamScope::isAutoScope);

        Integer scopeId = teamScopes.get(0).getId();
        List<TeamProfile> teamProfiles = ps.listTeams(scopeId);
        Assertions.assertThat(teamProfiles)
                .isNotNull()
                .hasSize(1)
                .allMatch(TeamProfile::isAutoTeam);

        Integer teamId = teamProfiles.get(0).getId();
        List<UserProfile> userProfiles = ps.listUsers(teamId, 0, 100);
        Assertions.assertThat(userProfiles)
                .isNotNull()
                .hasSize(4)        // (admin, curator, player) + default
                .allMatch(UserProfile::isAutoUser);

        Assertions.assertThat(userProfiles)
                .extracting("email")
                .contains(DefaultEntities.DEF_ADMIN_USER,
                        DefaultEntities.DEF_CURATOR_USER,
                        DefaultEntities.DEF_PLAYER_USER,
                        DefaultEntities.deriveDefScopeUser("default"));
    }

    @Test
    public void testAddUserProfileFailures() {
        {
            // add without email should fail
            Assertions.assertThatThrownBy(
                    () -> ps.addUserProfile(createProfile("test add user", null)))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(
                    () -> ps.addUserProfile(createProfile(null, "isuru@domain.com")))
                    .isInstanceOf(InputValidationException.class);

            // no reserved users should be able to add again
            Assertions.assertThatThrownBy(
                    () -> ps.addUserProfile(createProfile("Admin", "admin@oasis.com")))
                    .isInstanceOf(DbException.class);
            Assertions.assertThatThrownBy(
                    () -> ps.addUserProfile(createProfile("Curator", "curator@oasis.com")))
                    .isInstanceOf(DbException.class);
            Assertions.assertThatThrownBy(
                    () -> ps.addUserProfile(createProfile("Player", "player@oasis.com")))
                    .isInstanceOf(DbException.class);
            Assertions.assertThatThrownBy(
                    () -> ps.addUserProfile(createProfile("Default",
                            DefaultEntities.deriveDefScopeUser("default"))))
                    .isInstanceOf(DbException.class);
        }
    }

    @Test
    public void testAddUserProfile() throws Exception {
        {
            UserProfile profile = createProfile("Isuru Weerarathna", "isuru@dmain.com");
            profile.setMale(true);
            profile.setActivated(true);
            profile.setExtId(1001L);
            profile.setNickName("isuru");

            long id = ps.addUserProfile(profile);
            Assertions.assertThat(id).isGreaterThan(0);

            UserProfile ap = ps.readUserProfile(id);
            Assertions.assertThat(ap).isNotNull();
            SoftAssertions asserts = new SoftAssertions();
            asserts.assertThat(ap.getId()).isEqualTo(id);
            asserts.assertThat(ap.getEmail()).isEqualTo(profile.getEmail());
            asserts.assertThat(ap.getName()).isEqualTo(profile.getName());
            asserts.assertThat(ap.getExtId()).isEqualTo(profile.getExtId());
            asserts.assertThat(ap.getNickName()).isEqualTo(profile.getNickName());
            asserts.assertThat(ap.isActive()).isTrue();
            asserts.assertThat(ap.isActivated()).isTrue();
            asserts.assertThat(ap.getLastLogoutAt()).isNull();
            asserts.assertThat(ap.getPassword()).isNull();
            asserts.assertThat(ap.getAvatarId()).isNull();
            asserts.assertThat(ap.getHeroId()).isNull();
            asserts.assertThat(ap.getHeroLastUpdatedAt()).isNull();
            asserts.assertThat(ap.getHeroUpdateTimes()).isEqualTo(0);
            asserts.assertAll();

            // should not be able to add the same user again.
            UserProfile dupProfile = createProfile("Isuru Madushanka", profile.getEmail());
            profile.setMale(true);
            profile.setActivated(true);
            profile.setExtId(1001L);
            profile.setNickName("isuru");

            Assertions.assertThatThrownBy(() -> ps.addUserProfile(dupProfile))
                    .isInstanceOf(DbException.class);
        }
    }

    @Test
    public void testAddTeamScopeFailures() throws Exception {
        {
            // cannot add default team scope again
            Assertions.assertThatThrownBy(
                    () -> ps.addTeamScope(createScope(DefaultEntities.DEFAULT_TEAM_SCOPE_NAME,
                            "Default Scope", 1L)))
                    .isInstanceOf(DbException.class);
        }

        {
            TeamScope scope = createScope("HR", "Human Resource", 100L);
            Assertions.assertThat(ps.addTeamScope(scope)).isGreaterThan(0);

            // adding same scope again should fail
            Assertions.assertThatThrownBy(() -> ps.addTeamScope(scope))
                    .isInstanceOf(DbException.class);

            // adding a different scope with same name must fail
            Assertions.assertThatThrownBy(
                    () -> ps.addTeamScope(createScope("HR", "HumanResource", 101L)))
                    .isInstanceOf(DbException.class);
        }
    }

    @Test
    public void testAddTeamScope() throws Exception {
        {
            TeamScope scope = createScope("HR", "Human Resource", 123L);

            long sid = ps.addTeamScope(scope);
            Assertions.assertThat(sid).isGreaterThan(0);
            TeamScope ascp = ps.readTeamScope(sid);
            Assertions.assertThat(ascp).isNotNull();
            Assertions.assertThat(ascp.getName()).isEqualTo(scope.getName());
            Assertions.assertThat(ascp.getDisplayName()).isEqualTo(scope.getDisplayName());
            Assertions.assertThat(ascp.getExtId()).isEqualTo(scope.getExtId());
            Assertions.assertThat(ascp.isActive()).isTrue();
            Assertions.assertThat(ascp.getCreatedAt()).isBefore(new Date());
            Assertions.assertThat(ascp.getUpdatedAt()).isBefore(new Date());
            Assertions.assertThat(ascp.getUpdatedAt()).isEqualTo(ascp.getCreatedAt());

            // check default team exist
            List<TeamProfile> teamProfiles = ps.listTeams(sid);
            Assertions.assertThat(teamProfiles)
                    .hasSize(1)
                    .extracting("name")
                    .contains(DefaultEntities.deriveDefaultTeamName(SLUGIFY.slugify(scope.getName())));

            // check default user exists
            List<UserProfile> userProfiles = ps.listUsers(teamProfiles.get(0).getId(), 0, 10);
            Assertions.assertThat(userProfiles)
                    .hasSize(1)
                    .extracting("email")
                    .contains(DefaultEntities.deriveDefScopeUser(SLUGIFY.slugify(scope.getName())));


            {
                TeamScope scp1 = ps.readTeamScope(sid);
                TeamScope scp2 = ps.readTeamScope(scope.getName());
                Assertions.assertThat(scp1).isNotNull()
                    .hasFieldOrPropertyWithValue("name", scope.getName())
                    .hasFieldOrPropertyWithValue("displayName", scope.getDisplayName())
                    .hasFieldOrPropertyWithValue("extId", scope.getExtId());
                Assertions.assertThat(scp2).isNotNull()
                    .hasFieldOrPropertyWithValue("name", scope.getName())
                    .hasFieldOrPropertyWithValue("displayName", scope.getDisplayName())
                    .hasFieldOrPropertyWithValue("extId", scope.getExtId());
                Assertions.assertThat(scp1).isEqualTo(scp2);
            }
        }

        {
            // cannot add the same scope with name again
            TeamScope scope = createScope("HR", "Human Resource", 123L);

            Assertions.assertThatThrownBy(() -> ps.addTeamScope(scope))
                    .isInstanceOf(DbException.class);
        }

        {
            List<TeamScope> teamScopes = ps.listTeamScopes();
            Assertions.assertThat(teamScopes).isNotNull().hasSize(2);
        }
    }

    @Test
    public void testAddTeamFailures() throws Exception {
        TeamScope defScope = getDefScope();
        long hrId = ps.addTeamScope(createScope("HR", "Human Resource", 201L));

        {
            // should not be able to add default team again to default scope
            Assertions.assertThatThrownBy(
                    () -> ps.addTeam(createTeam(defScope.getId(), DefaultEntities.DEFAULT_TEAM_NAME)))
                    .isInstanceOf(DbException.class);

            // cannot add default team to other team scopes as well
            Assertions.assertThatThrownBy(
                    () -> ps.addTeam(createTeam(hrId, DefaultEntities.DEFAULT_TEAM_NAME)))
                    .isInstanceOf(DbException.class);
        }

        {
            // cannot add without name
            Assertions.assertThatThrownBy(() -> ps.addTeam(createTeam(hrId, null)))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.addTeam(createTeam(hrId, "")))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.addTeam(createTeam(hrId, "  ")))
                    .isInstanceOf(InputValidationException.class);

            // cannot add without scope id
            Assertions.assertThatThrownBy(() -> ps.addTeam(createTeam(null, "Empty Team")))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.addTeam(createTeam(0, "Empty Team")))
                    .isInstanceOf(InputValidationException.class);
        }
    }

    @Test
    public void testEditTeamScope() throws Exception {
        TeamScope defScope = getDefScope();

        {
            Assertions.assertThatThrownBy(
                    () -> ps.editTeamScope(0L, createScope("name", "Name", 1L)))
                    .isInstanceOf(InputValidationException.class);

            Assertions.assertThatThrownBy(
                    () -> ps.editTeamScope(-1L, createScope("name", "Name", 1L)))
                    .isInstanceOf(InputValidationException.class);
        }

        {
            // non existing id should fail
            Assertions.assertThatThrownBy(
                    () -> ps.editTeamScope(999999L,
                            createScope("name", "Name", 1L)))
                    .isInstanceOf(InputValidationException.class);
        }

        {
            // can edit display name
            TeamScope scopeToAdd = createScope(DefaultEntities.DEFAULT_TEAM_SCOPE_NAME,
                    "Edited : " + DefaultEntities.DEFAULT_TEAM_SCOPE_NAME, 234L);
            Assert.assertTrue(ps.editTeamScope(defScope.getId(), scopeToAdd));

            TeamScope readScope = ps.readTeamScope(defScope.getId());
            Assertions.assertThat(readScope.getDisplayName()).isEqualTo(scopeToAdd.getDisplayName());
            Assertions.assertThat(readScope.getName()).isEqualTo(defScope.getName());
            Assertions.assertThat(readScope.getUpdatedAt()).isAfter(readScope.getCreatedAt());
        }

        {
            // should not be able to change name at all
            TeamScope scopeToAdd = createScope("New Name",
                    "Edited : " + DefaultEntities.DEFAULT_TEAM_SCOPE_NAME, 234L);
            Assert.assertTrue(ps.editTeamScope(defScope.getId(), scopeToAdd));

            TeamScope readScope = ps.readTeamScope(defScope.getId());
            Assertions.assertThat(readScope.getDisplayName()).isEqualTo(scopeToAdd.getDisplayName());
            Assertions.assertThat(readScope.getName()).isEqualTo(defScope.getName());
            Assertions.assertThat(readScope.getUpdatedAt()).isAfter(readScope.getCreatedAt());
        }
    }

    @Test
    public void testAddTeam() throws Exception {
        TeamScope scopeHr = createScope("HR", "Human Resource", 120L);
        long hrId = ps.addTeamScope(scopeHr);
        TeamScope scopeFin = createScope("Finance", "Finance Dept.", 121L);
        long finId = ps.addTeamScope(scopeFin);

        {
            // without scope id, should fail
            TeamProfile team = createTeam(null, "Team 1");
            Assertions.assertThatThrownBy(() -> ps.addTeam(team))
                    .isInstanceOf(InputValidationException.class);
        }

        {
            // add first team
            TeamProfile hrTeam1 = createTeam(hrId, "HR - Team Recruiters");
            long tid = ps.addTeam(hrTeam1);
            Assertions.assertThat(tid).isGreaterThan(0);

            {
                List<UserProfile> userProfiles = ps.listUsers(tid, 0, 10);
                Assertions.assertThat(userProfiles).isNotNull()
                        .hasSize(1)
                        .extracting("email")
                        .contains(DefaultEntities.deriveDefTeamUser(SLUGIFY.slugify(hrTeam1.getName())));
            }

            {
                TeamProfile tp = ps.readTeam(tid);
                Assertions.assertThat(tp.getName()).isEqualTo(hrTeam1.getName());
                Assertions.assertThat(tp.getTeamScope()).isEqualTo(hrId);
                Assertions.assertThat(tp.getId()).isGreaterThan(0);
                Assertions.assertThat(tp.getAvatarId()).isNull();
                Assertions.assertThat(tp.getCreatedAt()).isBefore(new Date());
                Assertions.assertThat(tp.getUpdatedAt()).isBefore(new Date());
                Assertions.assertThat(tp.getUpdatedAt()).isEqualTo(tp.getCreatedAt());
            }

            // add other team
            TeamProfile finTeam1 = createTeam(finId, "Team Insurance");
            long fid = ps.addTeam(finTeam1);
            Assertions.assertThat(fid).isGreaterThan(0);

            {
                List<UserProfile> userProfiles = ps.listUsers(fid, 0, 10);
                Assertions.assertThat(userProfiles).isNotNull()
                        .hasSize(1)
                        .extracting("email")
                        .contains(DefaultEntities.deriveDefTeamUser(SLUGIFY.slugify(finTeam1.getName())));
            }

            {
                TeamProfile tp = ps.readTeam(fid);
                Assertions.assertThat(tp.getName()).isEqualTo(finTeam1.getName());
                Assertions.assertThat(tp.getTeamScope()).isEqualTo(finId);
                Assertions.assertThat(tp.getId()).isGreaterThan(0);
                Assertions.assertThat(tp.getAvatarId()).isNull();
                Assertions.assertThat(tp.getCreatedAt()).isBefore(new Date());
                Assertions.assertThat(tp.getUpdatedAt()).isBefore(new Date());
                Assertions.assertThat(tp.getUpdatedAt()).isEqualTo(tp.getCreatedAt());
            }

            // now there must have one team in each scope
            Assertions.assertThat(ps.listTeams(hrId)).isNotNull().hasSize(2);
            Assertions.assertThat(ps.listTeams(finId)).isNotNull().hasSize(2);
        }

        {
            Assert.assertNotNull(ps.findTeamByName("HR - Team Recruiters"));
            Assert.assertNotNull(ps.findTeamByName("Hr - Team Recruiters"));
            Assert.assertNotNull(ps.findTeamByName("hr - team recruiters"));
            Assert.assertNull(ps.findTeamByName("hr - teamrecruiters"));
        }
    }

    @Test
    public void testEditTeam() throws Exception {
        TeamScope defScope = getDefScope();
        TeamProfile defTeam = ps.listTeams(defScope.getId()).stream()
                .filter(TeamProfile::isAutoTeam).findFirst()
                .orElseThrow(() -> new IllegalStateException("No default team found!"));

        {
            // invalid team Ids
            Assertions.assertThatThrownBy(
                    () -> ps.editTeam(0L, createTeam(defScope.getId(), "temp")))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(
                    () -> ps.editTeam(-1L, createTeam(defScope.getId(), "temp")))
                    .isInstanceOf(InputValidationException.class);

            // non existing team Ids
            Assertions.assertThatThrownBy(
                    () -> ps.editTeam(999999L, createTeam(defScope.getId(), "temp")))
                    .isInstanceOf(InputValidationException.class);
        }

        {
            // Must not allow to edit default team name
            TeamProfile defNew = createTeam(defScope.getId(), "New Default");
            Assertions.assertThatThrownBy(() -> ps.editTeam(defTeam.getId(), defNew))
                    .isInstanceOf(InputValidationException.class);

            // but can edit avatar id
            defNew.setName(DefaultEntities.DEFAULT_TEAM_NAME);
            defNew.setAvatarId("img/newAvatarId.jpg");
            Assert.assertTrue(ps.editTeam(defTeam.getId(), defNew));

            {
                TeamProfile modTeam = ps.readTeam(defTeam.getId());
                Assertions.assertThat(modTeam.getId()).isEqualTo(defTeam.getId());
                Assertions.assertThat(modTeam.getName()).isEqualTo(defTeam.getName());
                Assertions.assertThat(modTeam.getAvatarId()).isEqualTo(defNew.getAvatarId());
                Assertions.assertThat(modTeam.getTeamScope()).isEqualTo(defTeam.getTeamScope());
                Assertions.assertThat(modTeam.getUpdatedAt()).isAfter(defTeam.getCreatedAt());
            }
        }
    }

    @Test
    public void testReadUser() throws Exception {
        TeamScope north = createScope("North", "North Region", 7L);
        long northId = ps.addTeamScope(north);
        TeamProfile winterfell = createTeam(northId, "Winterfell");
        long wid = ps.addTeam(winterfell);

        Assertions.assertThat(ps.listTeams(northId)).hasSize(2);
        Assertions.assertThat(ps.listUsers(wid, 0, 10)).hasSize(1);

        UserProfile nedStark = createProfile("Ned Stark", "ned@winterfell.com");
        nedStark.setExtId(30001L);
        long nedId = ps.addUserProfile(nedStark);
        UserProfile branStark = createProfile("Bran Stark", "bran@winterfell.com");
        branStark.setExtId(30005L);
        long branId = ps.addUserProfile(branStark);

        {
            // still there should only be one user in team winterfell,
            // because we did not add two users to the team yet.
            Assertions.assertThat(ps.listUsers(wid, 0, 10)).hasSize(1);
        }

        {
            // invalid ids must fail
            Assertions.assertThatThrownBy(() -> ps.readUserProfile(0L))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.readUserProfile(-1L))
                    .isInstanceOf(InputValidationException.class);

            // get user by id
            UserProfile ned = ps.readUserProfile(nedId);
            Assertions.assertThat(ned.getId()).isEqualTo(nedId);
            Assertions.assertThat(ned.getName()).isEqualTo(nedStark.getName());
            Assertions.assertThat(ned.getEmail()).isEqualTo(nedStark.getEmail());

            UserProfile bran = ps.readUserProfile(branId);
            Assertions.assertThat(bran.getId()).isEqualTo(branId);
            Assertions.assertThat(bran.getName()).isEqualTo(branStark.getName());
            Assertions.assertThat(bran.getEmail()).isEqualTo(branStark.getEmail());

            // non existing ids should return null
            Assertions.assertThat(ps.readUserProfile(99999L)).isNull();
        }

        {
            // invalid email ids must fail
            Assertions.assertThatThrownBy(() -> ps.readUserProfile(null))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.readUserProfile(""))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.readUserProfile("  "))
                    .isInstanceOf(InputValidationException.class);

            // read user by email
            UserProfile ned = ps.readUserProfile(nedStark.getEmail());
            Assertions.assertThat(ned.getId()).isEqualTo(nedId);
            Assertions.assertThat(ned.getName()).isEqualTo(nedStark.getName());
            Assertions.assertThat(ned.getEmail()).isEqualTo(nedStark.getEmail());

            UserProfile bran = ps.readUserProfile(branStark.getEmail());
            Assertions.assertThat(bran.getId()).isEqualTo(branId);
            Assertions.assertThat(bran.getName()).isEqualTo(branStark.getName());
            Assertions.assertThat(bran.getEmail()).isEqualTo(branStark.getEmail());

            // non existing emails should return null
            Assertions.assertThat(ps.readUserProfile("isuru@winterfell.com")).isNull();
            Assertions.assertThat(ps.readUserProfile("Ned Stark")).isNull();
        }

        {
            // invalid ids must fail
            Assertions.assertThatThrownBy(() -> ps.readUserProfile(0L))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.readUserProfile(-1L))
                    .isInstanceOf(InputValidationException.class);

            // read user by ext id
            UserProfile ned = ps.readUserProfileByExtId(nedStark.getExtId());
            Assertions.assertThat(ned.getId()).isEqualTo(nedId);
            Assertions.assertThat(ned.getName()).isEqualTo(nedStark.getName());
            Assertions.assertThat(ned.getEmail()).isEqualTo(nedStark.getEmail());

            UserProfile bran = ps.readUserProfileByExtId(branStark.getExtId());
            Assertions.assertThat(bran.getId()).isEqualTo(branId);
            Assertions.assertThat(bran.getName()).isEqualTo(branStark.getName());
            Assertions.assertThat(bran.getEmail()).isEqualTo(branStark.getEmail());

            // non existing ids should return null
            Assertions.assertThat(ps.readUserProfile(99999L)).isNull();
        }
    }

    @Test
    public void testTransferTeam() throws Exception {
        TeamScope river = createScope("River", "The River", 9L);
        long riverId = ps.addTeamScope(river);
        Assert.assertTrue(riverId > 0);

        TeamScope north = createScope("North", "North Region", 7L);
        long northId = ps.addTeamScope(north);
        Assert.assertTrue(northId > 0);

        TeamProfile winterfell = createTeam(northId, "Winterfell");
        long wid = ps.addTeam(winterfell);
        TeamProfile tully = createTeam(riverId, "Tully");
        long tid = ps.addTeam(tully);

        Assertions.assertThat(ps.listTeams(northId)).hasSize(2);
        Assertions.assertThat(ps.listTeams(riverId)).hasSize(2);
        Assertions.assertThat(ps.listUsers(wid, 0, 10)).hasSize(1);
        Assertions.assertThat(ps.listUsers(tid, 0, 10)).hasSize(1);

        UserProfile catelyn = createProfile("Catelyn Tully", "catelyn@tully.com");
        catelyn.setExtId(90001L);
        long catId = ps.addUserProfile(catelyn);
        Assert.assertTrue(catId > 0);

        UserProfile nedStark = createProfile("Ned Stark", "ned@winterfell.com");
        nedStark.setExtId(30001L);
        long nedId = ps.addUserProfile(nedStark);
        Assert.assertTrue(nedId > 0);

        {
            // add catelyn to tully team
            Assert.assertTrue(ps.addUserToTeam(catId, tid, UserRole.PLAYER, FALSE));

            // catelyn should be in tully team
            UserTeam catTeam = ps.findCurrentTeamOfUser(catId);
            Assert.assertNotNull(catTeam);
            Assert.assertEquals(tid, catTeam.getTeamId().longValue());
            Assert.assertEquals(catId, catTeam.getUserId().longValue());
            Assert.assertEquals(UserRole.PLAYER, catTeam.getRoleId().intValue());
            Assert.assertTrue(catTeam.isApproved());

            // transfer catelyn to Stark team, with pending approval
            long transferTime = catTeam.getJoinedTime() + (ONE_DAY * 7);

            Assert.assertTrue(ps.addUserToTeam(catId, wid, UserRole.CURATOR, TRUE, transferTime));

            // catelyn current team still must be tully
            Assert.assertEquals(catTeam, ps.findCurrentTeamOfUser(catId, TRUE, transferTime));

            Iterable<Map<String, Object>> assigns = dao.executeRawQuery("SELECT * FROM OA_TEAM_USER WHERE user_id = :userId",
                    Maps.create("userId", catId));
            System.out.println(assigns);

            // without approval state it must be stark
            UserTeam pendingTeam = ps.findCurrentTeamOfUser(catId, FALSE, transferTime);
            Assert.assertNotNull(pendingTeam);
            Assert.assertEquals(wid, pendingTeam.getTeamId().longValue());
            Assert.assertEquals(catId, pendingTeam.getUserId().longValue());
            Assert.assertEquals(UserRole.CURATOR, pendingTeam.getRoleId().intValue());
            Assert.assertFalse(pendingTeam.isApproved());
        }
    }

    @Test
    public void testAssignUsers() throws Exception {
        TeamScope rock = createScope("Rock", "The Rock", 8L);
        long rockId = ps.addTeamScope(rock);
        Assert.assertTrue(rockId > 0);
        TeamProfile casterlyRock = createTeam(rockId, "Casterly Rock");
        long cid = ps.addTeam(casterlyRock);
        Assert.assertTrue(cid > 0);

        Assertions.assertThat(ps.listTeams(rockId)).hasSize(2);
        Assertions.assertThat(ps.listUsers(cid, 0, 10)).hasSize(1);

        UserProfile tywin = createProfile("Tywin Lannister", "tywin@caterlyrock.com");
        tywin.setExtId(40001L);
        long tywinId = ps.addUserProfile(tywin);
        UserProfile jaime = createProfile("Jaime Lannister", "jaime@caterlyrock.com");
        jaime.setExtId(40005L);
        long jaimeId = ps.addUserProfile(jaime);

        {
            // @TODO before assignment user must be in default team of oasis.
            UserTeam currentTeamOfUser = ps.findCurrentTeamOfUser(tywinId);
            Assert.assertNull(currentTeamOfUser);   // @TODO fix to not null and check for default team

            // cannot add with invalid roles
            Assertions.assertThatThrownBy(() -> ps.addUserToTeam(tywinId, cid, 0))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.addUserToTeam(tywinId, cid, UserRole.ALL_ROLE + 1))
                    .isInstanceOf(InputValidationException.class);

            // add to team by default approved
            Assert.assertTrue(ps.addUserToTeam(tywinId, cid, UserRole.CURATOR, false));

            // now there must be two members of team
            Assertions.assertThat(ps.listUsers(cid, 0, 10))
                    .hasSize(2)
                    .extracting("email")
                    .contains(tywin.getEmail());

            // tywin's current team must be casterly rock
            {
                UserTeam tywinTeam = ps.findCurrentTeamOfUser(tywinId);
                Assertions.assertThat(tywinTeam)
                        .isNotNull()
                        .hasFieldOrPropertyWithValue("roleId", UserRole.CURATOR)
                        .hasFieldOrPropertyWithValue("userId", tywinId)
                        .hasFieldOrPropertyWithValue("teamId", (int) cid)
                        .hasFieldOrPropertyWithValue("deallocatedTime", null)
                        .hasFieldOrPropertyWithValue("approved", true)
                        .hasFieldOrPropertyWithValue("autoTeam", false);
                Assertions.assertThat(tywinTeam.getJoinedTime()).isLessThan(System.currentTimeMillis());

                // same should be returned with regardless of approved status
                Assert.assertEquals(ps.findCurrentTeamOfUser(tywinId, TRUE), tywinTeam);
                Assert.assertEquals(ps.findCurrentTeamOfUser(tywinId, FALSE), tywinTeam);

                // same should be returned null for past timestamps
                // @TODO actually must be in default team
                long assignStartTime = tywinTeam.getJoinedTime();
                Assert.assertNull(ps.findCurrentTeamOfUser(tywinId, TRUE, assignStartTime - 1));
                Assert.assertNull(ps.findCurrentTeamOfUser(tywinId, FALSE, assignStartTime - 1));

                // current team should return for after assign or future timestamps
                Assert.assertEquals(ps.findCurrentTeamOfUser(tywinId, TRUE, assignStartTime),
                        tywinTeam);
                Assert.assertEquals(ps.findCurrentTeamOfUser(tywinId, TRUE, assignStartTime + 1),
                        tywinTeam);
                Assert.assertEquals(ps.findCurrentTeamOfUser(tywinId, FALSE, assignStartTime + 3600L),
                        tywinTeam);
            }
        }

        {
            // adding tywin to the same team with same role, should return unsuccessful state,
            // regardless of approval state
            Assert.assertFalse(ps.addUserToTeam(tywinId, cid, UserRole.CURATOR, FALSE));
            Assert.assertFalse(ps.addUserToTeam(tywinId, cid, UserRole.CURATOR, TRUE));

            UserTeam prevTywinTeam = ps.findCurrentTeamOfUser(tywinId);
            Assert.assertNotNull(prevTywinTeam);

            // but with a different role, should return successful
            long newAssignedTime = prevTywinTeam.getJoinedTime() + ONE_DAY;
            Assert.assertTrue(ps.addUserToTeam(tywinId, cid, UserRole.PLAYER, FALSE, newAssignedTime));

            // new role should have changed
            UserTeam tywinWithNewRole = ps.findCurrentTeamOfUser(tywinId, TRUE, newAssignedTime);
            Assert.assertNotNull(tywinWithNewRole);
            Assert.assertEquals(UserRole.PLAYER, tywinWithNewRole.getRoleId().intValue());

            // and it should be a new record.
            Assertions.assertThat(tywinWithNewRole.getId()).isNotEqualTo(prevTywinTeam.getId());
            Assert.assertEquals(tywinWithNewRole.getTeamId(), prevTywinTeam.getTeamId());
            Assert.assertEquals(tywinWithNewRole.getScopeId(), prevTywinTeam.getScopeId());
            Assert.assertEquals(tywinWithNewRole.getUserId(), prevTywinTeam.getUserId());
            Assertions.assertThat(tywinWithNewRole.getJoinedTime()).isGreaterThan(prevTywinTeam.getJoinedTime());

            Assert.assertEquals(prevTywinTeam, ps.findCurrentTeamOfUser(tywinId, TRUE, newAssignedTime - 1));
            Assert.assertEquals(prevTywinTeam, ps.findCurrentTeamOfUser(tywinId, FALSE, newAssignedTime - 1));
        }
    }

    @Test
    public void testDeleteUsers() throws Exception {
        TeamScope north = createScope("North", "North Region", 7L);
        long northId = ps.addTeamScope(north);
        TeamProfile winterfell = createTeam(northId, "Winterfell");
        long wid = ps.addTeam(winterfell);

        Assertions.assertThat(ps.listTeams(northId)).hasSize(2);
        Assertions.assertThat(ps.listUsers(wid, 0, 10)).hasSize(1);

        UserProfile nedStark = createProfile("Ned Stark", "ned@winterfell.com");
        nedStark.setExtId(30001L);
        long nedId = ps.addUserProfile(nedStark);
        UserProfile branStark = createProfile("Bran Stark", "bran@winterfell.com");
        branStark.setExtId(30005L);
        long branId = ps.addUserProfile(branStark);
        UserProfile aryaStark = createProfile("Arya Stark", "arya@winterfell.com");
        aryaStark.setExtId(30006L);
        long aryaId = ps.addUserProfile(aryaStark);

        {
            Assert.assertTrue(ps.addUserToTeam(nedId, wid, UserRole.CURATOR));
            Assert.assertTrue(ps.addUserToTeam(branId, wid, UserRole.PLAYER));
            Assert.assertTrue(ps.addUserToTeam(aryaId, wid, UserRole.PLAYER));

            Assertions.assertThat(ps.listUsers(wid, 0, 10)).hasSize(4);

            Assertions.assertThatThrownBy(() -> ps.findUser(null, null))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.findUser("", null))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(() -> ps.findUser("  ", null))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThat(ps.findUser("a", null)).isEmpty();
            Assertions.assertThat(ps.findUser("br", null)).isEmpty();
            Assertions.assertThat(ps.findUser("ary", null)).isEmpty();
            Assertions.assertThat(ps.findUser("stark", null)).isEmpty();
            Assertions.assertThat(ps.findUser("fell", null)).hasSize(4)
                    .filteredOn(UserProfile::isAutoUser).hasSize(1);
            Assertions.assertThat(ps.findUser("ned@", null)).hasSize(1);
            Assertions.assertThat(ps.findUser("ned%", null)).isEmpty();
            Assertions.assertThat(ps.findUser("stark", "stark")).hasSize(3);
            Assertions.assertThat(ps.findUser("stark", "st")).isEmpty();
            Assertions.assertThat(ps.findUser("arya", "arya")).hasSize(1);
        }

        {
            // delete arya stark from winterfell
            Assert.assertTrue(ps.deleteUserProfile(aryaId));

            // list users should not return inactive users
            Assertions.assertThat(ps.listUsers(wid, 0, 10)).hasSize(3);

            // user should not return in search lists
            Assertions.assertThat(ps.findUser("arya", "arya")).isEmpty();

            // but user can be read using raw id
            Assert.assertNotNull(ps.readUserProfile(aryaId));

            // users can repeatedly delete same user again and again. no difference
            Assert.assertTrue(ps.deleteUserProfile(aryaId));
        }
    }

    @Test
    public void testEditUsers() throws Exception {
        TeamScope north = createScope("North", "North Region", 7L);
        long northId = ps.addTeamScope(north);
        TeamProfile winterfell = createTeam(northId, "Winterfell");
        long wid = ps.addTeam(winterfell);

        Assertions.assertThat(ps.listTeams(northId)).hasSize(2);
        Assertions.assertThat(ps.listUsers(wid, 0, 10)).hasSize(1);

        {
            // non existing user ids
            Assertions.assertThatThrownBy(
                    () -> ps.editUserProfile(9999L, createProfile("", "")))
                    .isInstanceOf(InputValidationException.class);
        }

        UserProfile nedStark = createProfile("Ned Stark", "ned@winterfell.com");
        nedStark.setExtId(30001L);
        long nedId = ps.addUserProfile(nedStark);
        UserProfile branStark = createProfile("Bran Stark", "bran@winterfell.com");
        branStark.setExtId(30005L);
        branStark.setMale(true);
        long branId = ps.addUserProfile(branStark);
        UserProfile aryaStark = createProfile("Arya Stark", "arya@winterfell.com");
        aryaStark.setExtId(30006L);
        long aryaId = ps.addUserProfile(aryaStark);

        {
            // invalid user ids
            Assertions.assertThatThrownBy(
                    () -> ps.editUserProfile(0L, createProfile("", "")))
                    .isInstanceOf(InputValidationException.class);
            Assertions.assertThatThrownBy(
                    () -> ps.editUserProfile(-1L, createProfile("", "")))
                    .isInstanceOf(InputValidationException.class);

        }

        {
            // edit name and avatar id
            UserProfile editedProfile = createProfile("Brandan Stark", branStark.getEmail());
            editedProfile.setAvatarId("/img/nightking.jpg");
            editedProfile.setMale(true);
            Assert.assertTrue(ps.editUserProfile(branId, editedProfile));

            UserProfile branNew = ps.readUserProfile(branId);
            Assert.assertEquals(editedProfile.getName(), branNew.getName());
            Assert.assertEquals(editedProfile.getAvatarId(), branNew.getAvatarId());
            Assert.assertEquals(editedProfile.getEmail(), branStark.getEmail());
            Assert.assertEquals(editedProfile.getNickName(), branStark.getNickName());
            Assert.assertTrue(branNew.isMale());

            // edit nick name
            UserProfile p2 = createProfile(branNew.getName(), branNew.getEmail());
            p2.setNickName("Nightking");
            p2.setMale(true);
            Assert.assertTrue(ps.editUserProfile(branId, p2));

            UserProfile ep2 = ps.readUserProfile(branId);
            Assert.assertEquals(ep2.getName(), branNew.getName());
            Assert.assertEquals(ep2.getAvatarId(), branNew.getAvatarId());
            Assert.assertEquals(ep2.getEmail(), branStark.getEmail());
            Assert.assertEquals(ep2.getNickName(), p2.getNickName());
            Assert.assertTrue(ep2.isMale());
        }

        {
            // change gender
            UserProfile p1 = createProfile(aryaStark.getName(), aryaStark.getEmail());
            p1.setMale(false);
            Assert.assertTrue(ps.editUserProfile(aryaId, p1));

            UserProfile ep1 = ps.readUserProfile(aryaId);
            Assert.assertEquals(ep1.getName(), aryaStark.getName());
            Assert.assertEquals(ep1.getAvatarId(), aryaStark.getAvatarId());
            Assert.assertEquals(ep1.getEmail(), aryaStark.getEmail());
            Assert.assertEquals(ep1.getNickName(), aryaStark.getNickName());
            Assert.assertFalse(ep1.isMale());
        }

    }

    private TeamScope getDefScope() throws Exception {
        return ps.listTeamScopes().stream()
                .filter(TeamScope::isAutoScope)
                .findFirst().orElseThrow(IllegalStateException::new);
    }

    private TeamProfile createTeam(Number scopeId, String name) {
        TeamProfile team = new TeamProfile();
        if (scopeId != null) {
            team.setTeamScope(scopeId.intValue());
        }
        team.setName(name);
        return team;
    }

    private TeamScope createScope(String name, String displayName, Long extId) {
        TeamScope scope = new TeamScope();
        scope.setDisplayName(displayName);
        scope.setName(name);
        scope.setExtId(extId);
        return scope;
    }

    private UserProfile createProfile(String username, String email) {
        UserProfile profile = new UserProfile();
        profile.setName(username);
        profile.setEmail(email);
        return profile;
    }

}