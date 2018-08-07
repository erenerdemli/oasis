package io.github.isuru.oasis.game.persist.rabbit;

import io.github.isuru.oasis.game.persist.OasisSink;
import io.github.isuru.oasis.model.configs.ConfigKeys;
import io.github.isuru.oasis.model.configs.Configs;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.Serializable;

/**
 * @author iweerarathna
 */
public class OasisRabbitSink extends OasisSink implements Serializable {

    private RMQConnectionConfig config;

    private final String pointQueue;
    private final String milestoneQueue;
    private final String milestoneStateQueue;
    private final String badgeQueue;
    private final String challengeQueue;

    private final Configs gameProperties;

    public OasisRabbitSink(Configs gameProps) {
        this.gameProperties = gameProps;

        pointQueue = gameProps.getStr(ConfigKeys.KEY_RABBIT_QUEUE_OUT_POINTS, "game.o.points");
        milestoneQueue = gameProps.getStr(ConfigKeys.KEY_RABBIT_QUEUE_OUT_MILESTONES, "game.o.milestones");
        milestoneStateQueue = gameProps.getStr(ConfigKeys.KEY_RABBIT_QUEUE_OUT_MILESTONESTATES, "game.o.milestonestates");
        badgeQueue = gameProps.getStr(ConfigKeys.KEY_RABBIT_QUEUE_OUT_BADGES, "game.o.badges");
        challengeQueue = gameProps.getStr(ConfigKeys.KEY_RABBIT_QUEUE_OUT_CHALLENGES, "game.o.challenges");

        config = RabbitUtils.createRabbitConfig(gameProps);
    }

    @Override
    public SinkFunction<String> createPointSink() {
        return new RMQOasisSink<>(config, pointQueue, new SimpleStringSchema(), gameProperties);
    }

    @Override
    public SinkFunction<String> createMilestoneSink() {
        return new RMQOasisSink<>(config, milestoneQueue, new SimpleStringSchema(), gameProperties);
    }

    @Override
    public SinkFunction<String> createMilestoneStateSink() {
        return new RMQOasisSink<>(config, milestoneStateQueue, new SimpleStringSchema(), gameProperties);
    }

    @Override
    public SinkFunction<String> createBadgeSink() {
        return new RMQOasisSink<>(config, badgeQueue, new SimpleStringSchema(), gameProperties);
    }

    @Override
    public SinkFunction<String> createChallengeSink() {
        return new RMQOasisSink<>(config, challengeQueue, new SimpleStringSchema(), gameProperties);
    }

}