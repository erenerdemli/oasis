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

package io.github.oasis.core.services.api.services;

import com.mysql.cj.exceptions.AssertionFailedException;
import io.github.oasis.core.Game;
import io.github.oasis.core.configs.OasisConfigs;
import io.github.oasis.core.elements.ElementDef;
import io.github.oasis.core.exception.OasisException;
import io.github.oasis.core.exception.OasisRuntimeException;
import io.github.oasis.core.external.Db;
import io.github.oasis.core.external.messages.GameState;
import io.github.oasis.core.model.EventSource;
import io.github.oasis.core.services.api.TestUtils;
import io.github.oasis.core.services.api.beans.BackendRepository;
import io.github.oasis.core.services.api.beans.KeyGeneratorHelper;
import io.github.oasis.core.services.api.beans.StatsApiContext;
import io.github.oasis.core.services.api.beans.jdbc.JdbcRepository;
import io.github.oasis.core.services.api.controllers.admin.ElementsController;
import io.github.oasis.core.services.api.controllers.admin.EventSourceController;
import io.github.oasis.core.services.api.controllers.admin.GamesController;
import io.github.oasis.core.services.api.dao.IElementDao;
import io.github.oasis.core.services.api.dao.IEventSourceDao;
import io.github.oasis.core.services.api.dao.IGameDao;
import io.github.oasis.core.services.api.exceptions.ErrorCodes;
import io.github.oasis.core.services.api.exceptions.OasisApiRuntimeException;
import io.github.oasis.core.services.api.to.ElementCreateRequest;
import io.github.oasis.core.services.api.to.EventSourceCreateRequest;
import io.github.oasis.core.services.api.to.GameCreateRequest;
import io.github.oasis.core.services.api.to.GameUpdateRequest;
import io.github.oasis.core.services.exceptions.OasisApiException;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Isuru Weerarathna
 */
public class GameServiceTest extends AbstractServiceTest {

    public static final String TESTPOINT = "testpoint";
    public static final String TESTBADGE = "testbadge";
    private GamesController controller;
    private ElementsController elementsController;
    private EventSourceController sourceController;

    private StatsApiContext statsApiContext;
    private final KeyGeneratorHelper keyGeneratorSupport = new KeyGeneratorHelper();
    private final IEngineManager engineManager = Mockito.mock(IEngineManager.class);

    private final GameCreateRequest stackOverflow = GameCreateRequest.builder()
            .name("Stack-overflow")
            .description("Stackoverflow badges and points system")
            .logoRef("https://oasis.io/assets/so.jpeg")
            .motto("Help the community")
            .build();

    private final GameCreateRequest promotions = GameCreateRequest.builder()
            .name("Promotions")
            .description("Provides promotions for customers based on their loyality")
            .logoRef("https://oasis.io/assets/pm.jpeg")
            .motto("Serve your customers")
            .build();

    public GameServiceTest() {
        try {
            keyGeneratorSupport.init();
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionFailedException("Cannot initialize key generator!");
        }
    }

    @Test
    void addGame() throws OasisException {
        Game game = controller.addGame(stackOverflow);
        System.out.println(game);
        Assertions.assertNotNull(game);
        assertGame(game, stackOverflow);
        assertEquals(GameState.CREATED.name(), game.getCurrentStatus());

        System.out.println(promotions);
        Game pGame = controller.addGame(promotions);
        assertGame(pGame, promotions);
        assertEquals(GameState.CREATED.name(), pGame.getCurrentStatus());

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> controller.addGame(stackOverflow))
                .isInstanceOf(OasisApiRuntimeException.class)
                .hasFieldOrPropertyWithValue("errorCode", ErrorCodes.GAME_ALREADY_EXISTS);
    }

    @Test
    void listGames() throws OasisException {
        assertEquals(0, controller.listGames("0", 50).getRecords().size());

        controller.addGame(stackOverflow);
        assertEquals(1, controller.listGames("0", 50).getRecords().size());

        controller.addGame(promotions);
        assertEquals(2, controller.listGames("0", 50).getRecords().size());

        Assertions.assertThrows(OasisApiRuntimeException.class, () -> controller.addGame(stackOverflow));
        assertEquals(2, controller.listGames("0", 50).getRecords().size());
    }

    @Test
    void updateGame() throws OasisException {
        Game stackGame = controller.addGame(stackOverflow);
        int stackId = stackGame.getId();

        assertGame(engineRepo.readGame(stackId), stackOverflow);
        assertGame(adminRepo.readGame(stackId), stackOverflow);

        GameUpdateRequest updateRequest = GameUpdateRequest.builder()
                .id(stackId)
                .motto("new motto")
                .description("new description")
                .logoRef("new logo ref")
                .build();
        Game updatedGame = controller.updateGame(stackId, updateRequest);
        assertGame(updatedGame, updateRequest);
        assertEquals(GameState.CREATED.name(), updatedGame.getCurrentStatus());
    }

    @Test
    void shouldNotUpdateGameWithStatus() throws OasisException {
        Game stackGame = controller.addGame(stackOverflow);
        int stackId = stackGame.getId();

        assertGame(engineRepo.readGame(stackId), stackOverflow);
        assertGame(adminRepo.readGame(stackId), stackOverflow);

        GameUpdateRequest updateRequest = GameUpdateRequest.builder()
                .id(stackId)
                .motto("new motto")
                .description("new description")
                .logoRef("new logo ref")
                .build();
        Game updatedGame = controller.updateGame(stackId, updateRequest);
        assertGame(updatedGame, updateRequest);
        assertEquals(GameState.CREATED.name(), updatedGame.getCurrentStatus());
    }

    @Test
    void updateGameStatusOnly() throws OasisException {
        Game stackGame = controller.addGame(stackOverflow);
        int stackId = stackGame.getId();

        assertGame(engineRepo.readGame(stackId), stackOverflow);
        assertGame(adminRepo.readGame(stackId), stackOverflow);

        Game updatedGame = controller.updateGameStatus(stackId, GameState.STARTED.name());
        assertEquals(GameState.STARTED.name(), updatedGame.getCurrentStatus());
    }

    @Test
    void readGame() throws OasisException {
        int stackId = controller.addGame(stackOverflow).getId();
        Game stackGame = controller.readGame(stackId);
        assertGame(stackGame, stackOverflow);
    }

    @Test
    void readGameByName() throws OasisException {
        controller.addGame(stackOverflow);
        Game stackGame = controller.getGameByName(stackOverflow.getName());
        assertGame(stackGame, stackOverflow);
    }

    @Test
    void deleteGame() throws OasisException {
        Mockito.reset(engineManager);

        int stackId = controller.addGame(stackOverflow).getId();
        Game dbGame = controller.readGame(stackId);
        EventSource eventSource = sourceController.registerEventSource(EventSourceCreateRequest.builder().name("test-1").build());
        sourceController.associateEventSourceToGame(stackId, eventSource.getId());

        List<ElementCreateRequest> elementCreateRequests = TestUtils.parseElementRules("rules.yml", stackId);
        ElementDef elementPoint = elementsController.add(stackId, TestUtils.findById(TESTPOINT, elementCreateRequests));
        ElementDef elementBadge = elementsController.add(stackId, TestUtils.findById(TESTBADGE, elementCreateRequests));

        assertGame(engineRepo.readGame(stackId), stackOverflow);
        assertGame(adminRepo.readGame(stackId), stackOverflow);
        assertEquals(1, sourceController.getEventSourcesOfGame(stackId).size());
        assertEquals(elementBadge.getElementId(), elementsController.read(stackId, TESTBADGE, false).getElementId());
        assertEquals(elementPoint.getElementId(), elementsController.read(stackId, TESTPOINT, false).getElementId());

        assertNotNull(controller.deleteGame(stackId));

        assertTrue(sourceController.getEventSourcesOfGame(stackId).isEmpty());
        // but still event source must exist
        EventSource dbSource = sourceController.getEventSource(eventSource.getId());
        assertNotNull(dbSource);
        assertTrue(dbSource.isActive());
        assertThrows(OasisApiException.class, () -> elementsController.read(stackId, TESTBADGE, false));
        assertThrows(OasisApiException.class, () -> elementsController.read(stackId, TESTPOINT, false));

        // game stopped message should dispatch
        assertEngineManagerOnceCalledWithState(GameState.STOPPED, dbGame.toBuilder().currentStatus(GameState.STOPPED.name()).build());

        assertFalse(adminRepo.readGame(stackId).isActive());
        assertThrows(OasisRuntimeException.class, () -> engineRepo.readGame(stackId));
    }


    @Test
    void updateGameStatus() throws OasisException {
        int stackId = controller.addGame(stackOverflow).getId();

        Mockito.reset(engineManager);
        Game gameRef = controller.updateGameStatus(stackId, "start");
        assertEngineManagerOnceCalledWithState(GameState.STARTED, gameRef);

        Mockito.reset(engineManager);
        gameRef = controller.updateGameStatus(stackId, "stop");
        assertEngineManagerOnceCalledWithState(GameState.STOPPED, gameRef);

        Mockito.reset(engineManager);
        gameRef = controller.updateGameStatus(stackId, "pause");
        assertEngineManagerOnceCalledWithState(GameState.PAUSED, gameRef);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> controller.updateGameStatus(stackId, null))
                .isInstanceOf(OasisApiException.class)
                .hasFieldOrPropertyWithValue("errorCode", ErrorCodes.GAME_UNKNOWN_STATE);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> controller.updateGameStatus(stackId, ""))
                .isInstanceOf(OasisApiException.class)
                .hasFieldOrPropertyWithValue("errorCode", ErrorCodes.GAME_UNKNOWN_STATE);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> controller.updateGameStatus(stackId, "hello"))
                .isInstanceOf(OasisApiException.class)
                .hasFieldOrPropertyWithValue("errorCode", ErrorCodes.GAME_UNKNOWN_STATE);
    }

    private void assertEngineManagerOnceCalledWithState(GameState state, Game game) {
        Mockito.verify(engineManager,
                Mockito.times(1)).changeGameStatus(state, game);
    }

    @Override
    protected void prepareContext(Db dbPool, OasisConfigs configs) throws OasisException {
        statsApiContext = new StatsApiContext(dbPool, configs);
        statsApiContext.init();
    }

    @Override
    protected JdbcRepository createJdbcRepository(Jdbi jdbi) {
        return new JdbcRepository(
                jdbi.onDemand(IGameDao.class),
                jdbi.onDemand(IEventSourceDao.class),
                jdbi.onDemand(IElementDao.class),
                null,
                serializationSupport
        );
    }

    @Override
    protected void createServices(BackendRepository backendRepository) {
        controller = new GamesController(new GameService(backendRepository, engineManager));
        elementsController = new ElementsController(new ElementService(backendRepository, statsApiContext));
        sourceController = new EventSourceController(new EventSourceService(backendRepository, keyGeneratorSupport));
    }

    private void assertGame(Game db, GameCreateRequest other) {
        assertTrue(db.getId() > 0);
        assertEquals(other.getName(), db.getName());
        assertEquals(other.getDescription(), db.getDescription());
        assertEquals(other.getLogoRef(), db.getLogoRef());
        assertEquals(other.getMotto(), db.getMotto());
    }

    private void assertGame(Game db, GameUpdateRequest other) {
        assertTrue(db.getId() > 0);
        assertEquals(other.getDescription(), db.getDescription());
        assertEquals(other.getLogoRef(), db.getLogoRef());
        assertEquals(other.getMotto(), db.getMotto());
    }
}
