/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.oasis.core.context;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Isuru Weerarathna
 */
public class ExecutionContext implements Serializable {

    private int userTimeOffset;
    private String userTimeZone;
    private GameContext gameContext;

    // work still to do for the event this context belongs to, null when nobody waits for it
    private transient Completion completion;

    public GameContext getGameContext() {
        return gameContext;
    }

    /**
     * Counts one more piece of work for the event, such as a signal it produced or an event one
     * of its signals produced.
     */
    public void workStarted() {
        if (completion != null) {
            completion.pending.incrementAndGet();
        }
    }

    /**
     * Marks one piece of work for the event done. When the last one is done, the event is
     * finished: all its signals are consumed and its awards are written.
     */
    public void workFinished() {
        if (completion != null && completion.pending.decrementAndGet() == 0) {
            completion.onFinished.run();
        }
    }

    public int getUserTimeOffset() {
        return userTimeOffset;
    }

    public String getUserTimeZone() {
        return userTimeZone;
    }

    public static ExecutionContext withUserTz(int offSet, String timeZone) {
        ExecutionContext context = new ExecutionContext();
        context.userTimeOffset = offSet;
        context.userTimeZone = timeZone;
        return context;
    }

    public static Builder from(GameContext gameContext) {
        return new Builder().using(gameContext);
    }

    public static class Builder {
        private ExecutionContext context = new ExecutionContext();

        public ExecutionContext build() {
            return context;
        }

        public Builder using(GameContext gameContext) {
            context.gameContext = gameContext;
            return this;
        }

        public Builder withUserTz(int offSetInSeconds, String timeZone) {
            context.userTimeOffset = offSetInSeconds;
            context.userTimeZone = timeZone;
            return this;
        }

        /**
         * Runs the given action once the event and all the work it caused are done. The event
         * itself is the first piece of work, finished with {@link ExecutionContext#workFinished()}.
         */
        public Builder whenFinished(Runnable onFinished) {
            context.completion = new Completion(onFinished);
            return this;
        }
    }

    private static class Completion {

        private final AtomicInteger pending = new AtomicInteger(1);
        private final Runnable onFinished;

        private Completion(Runnable onFinished) {
            this.onFinished = onFinished;
        }
    }
}
