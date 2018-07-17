package io.github.isuru.oasis.db;

import io.github.isuru.oasis.model.Event;
import io.github.isuru.oasis.model.Milestone;
import io.github.isuru.oasis.model.handlers.BadgeNotification;
import io.github.isuru.oasis.model.handlers.PointNotification;

/**
 * @author iweerarathna
 */
public interface IGameDao {

    void addPoint(Long userId, PointNotification pointNotification) throws Exception;

    void addBadge(Long userId, BadgeNotification badgeNotification) throws Exception;

    void addMilestone(Long userId, int level, Event event, Milestone milestone) throws Exception;

    void addMilestoneCurrState(Long userId, Milestone milestone, double value) throws Exception;
    void addMilestoneCurrState(Long userId, Milestone milestone, long value) throws Exception;
}