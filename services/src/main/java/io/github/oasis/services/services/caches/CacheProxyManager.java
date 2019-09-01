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

package io.github.oasis.services.services.caches;

import io.github.oasis.model.utils.ICacheProxy;
import io.github.oasis.services.configs.OasisConfigurations;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class CacheProxyManager {

    private static final Logger LOG = LoggerFactory.getLogger(CacheProxyManager.class);

    private final ICacheProxy cacheProxy;

    @Autowired
    public CacheProxyManager(Map<String, ICacheProxy> cacheProxyMap, OasisConfigurations configurations) {
        String cacheImpl = configurations.getCache().getImpl();
        String key = "cache" + StringUtils.capitalize(cacheImpl);
        cacheProxy = cacheProxyMap.computeIfAbsent(key, s -> cacheProxyMap.get("cacheNone"));
        LOG.info("Loaded cache implementation: " + cacheProxy.getClass().getName());
    }

    public ICacheProxy get() {
        return cacheProxy;
    }
}
