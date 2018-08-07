package io.github.isuru.oasis.services.api.routers;

import io.github.isuru.oasis.model.collect.Pair;
import io.github.isuru.oasis.model.configs.Configs;
import io.github.isuru.oasis.services.api.IOasisApiService;
import io.github.isuru.oasis.services.exception.ApiAuthException;
import io.github.isuru.oasis.services.model.UserProfile;
import io.github.isuru.oasis.services.model.UserTeam;
import io.github.isuru.oasis.services.model.enums.UserRole;
import io.github.isuru.oasis.services.utils.AuthUtils;
import io.github.isuru.oasis.services.utils.Maps;
import spark.Request;
import spark.Response;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author iweerarathna
 */
public class AuthRouter extends BaseRouters {

    private static final Set<String> RESERVED_USERS = new HashSet<>(
            Arrays.asList("admin@oasis.com", "player@oasis.com", "curator@oasis.com"));

    public AuthRouter(IOasisApiService apiService) {
        super(apiService);
    }

    @Override
    public void register() {
        post("/login", this::login)
        .post("/logout", this::logout);
    }

    private Object logout(Request req, Response res) throws Exception {
        checkAuth(req);
        return Maps.create("success", true);
    }

    private Object login(Request req, Response res) throws Exception {
        Pair<String, String> basicAuthPair = getBasicAuthPair(req);
        String username = basicAuthPair.getValue0();
        String password = basicAuthPair.getValue1();

        if (!RESERVED_USERS.contains(username)) {
            AuthUtils.get().ldapAuthUser(username, password);
        }

        boolean fresh = false;
        // if ldap auth success
        UserProfile profile = getApiService().getProfileService().readUserProfile(username);
        if (profile == null) {
            // non-admin user first time login...
            if (!RESERVED_USERS.contains(username)) {
                // add to database
                UserProfile userNew = new UserProfile();
                userNew.setName(captureNameFromEmail(username));
                userNew.setEmail(username);
                userNew.setActive(true);
                long idNew = getApiService().getProfileService().addUserProfile(userNew);
                profile = getApiService().getProfileService().readUserProfile(idNew);
                fresh = true;
            } else {
                throw new ApiAuthException("Authentication failure! Username or password incorrect!");
            }
        }
        UserTeam team = getApiService().getProfileService().findCurrentTeamOfUser(profile.getId());
        int role = UserRole.PLAYER.getIndex();
        if (team != null) {
            role = team.getRoleId();
        }

        if (RESERVED_USERS.contains(username)) {
            if (role == UserRole.ADMIN.getIndex()) {
                // admin
                if (!password.equals(Configs.get().getStrReq("oasis.default.admin.password"))) {
                    throw new ApiAuthException("Username or password incorrect!");
                }
            } else if (role == UserRole.CURATOR.getIndex()) {
                // curator
                if (!password.equals(Configs.get().getStrReq("oasis.default.curator.password"))) {
                    throw new ApiAuthException("Username or password incorrect!");
                }
            } else if (role == UserRole.PLAYER.getIndex()) {
                // player
                if (!password.equals(Configs.get().getStrReq("oasis.default.player.password"))) {
                    throw new ApiAuthException("Username or password incorrect!");
                }
            } else {
                throw new ApiAuthException("You do not have a proper role in Oasis!");
            }
        }

        AuthUtils.TokenInfo info = new AuthUtils.TokenInfo();
        info.setAdmin(role == UserRole.ADMIN.getIndex());
        info.setCurator(role == UserRole.CURATOR.getIndex());
        info.setUser(profile.getId());
        info.setExp(AuthUtils.get().getExpiryDate());
        String token = AuthUtils.get().issueToken(info);
        return Maps.create()
                .put("token", token)
                .put("isNew", fresh)
                .put("profile", profile)
                .build();
    }

    private static String captureNameFromEmail(String email) {
        int pos = email.lastIndexOf('@');
        if (pos > 0) {
            return email.substring(0, pos);
        } else {
            return email;
        }
    }
}