package io.github.isuru.oasis.services.services.control;

import io.github.isuru.oasis.model.configs.Configs;
import io.github.isuru.oasis.model.defs.ChallengeDef;
import io.github.isuru.oasis.model.defs.GameDef;
import io.github.isuru.oasis.services.model.IEventDispatcher;
import io.github.isuru.oasis.services.model.IGameController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author iweerarathna
 */
@Component("schedulerRemote")
public class FlinkScheduler implements IGameController {

    @Autowired
    private IEventDispatcher eventDispatcher;

    public FlinkScheduler(IEventDispatcher eventDispatcher) {
        this.eventDispatcher = eventDispatcher;
    }

    @Override
    public void submitEvent(long gameId, Map<String, Object> event) throws Exception {
        eventDispatcher.dispatch(gameId, event);
    }

    @Override
    public void startGame(long gameId, Configs appConfigs) throws Exception {

    }

    @Override
    public void startChallenge(ChallengeDef challengeDef, Configs appConfigs) throws Exception {

    }

    @Override
    public void stopGame(long gameId) {

    }

    @Override
    public void resumeChallenge(ChallengeDef challengeDef, Configs appConfigs) throws Exception {

    }

    @Override
    public void resumeGame(GameDef gameDef, Configs appConfigs) throws Exception {

    }

}