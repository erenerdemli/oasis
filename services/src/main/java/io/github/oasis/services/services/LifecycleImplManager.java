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

package io.github.oasis.services.services;

import io.github.oasis.model.utils.OasisUtils;
import io.github.oasis.services.configs.OasisConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class LifecycleImplManager {

    private static final Logger LOG = LoggerFactory.getLogger(LifecycleImplManager.class);

    private ILifecycleService activatedLifecycle;

    @Autowired
    public LifecycleImplManager(Map<String, ILifecycleService> lifecycleServiceMap, OasisConfigurations configurations) {
        boolean local = OasisUtils.getEnvOr("OASIS_MODE", "oasis.mode", configurations.getMode())
                .trim()
                .equalsIgnoreCase("local");

        if (local) {
            LOG.info("Activating local lifecycle service...");
            activatedLifecycle = lifecycleServiceMap.get("localLifecycleService");
        } else {
            LOG.info("Activating remote lifecycle service...");
            activatedLifecycle = lifecycleServiceMap.get("remoteLifecycleService");
        }
    }

    public ILifecycleService get() {
        return activatedLifecycle;
    }

}