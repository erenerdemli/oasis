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

package io.github.oasis.engine.ext;

import akka.actor.AbstractExtensionId;
import akka.actor.ExtendedActorSystem;
import akka.actor.ExtensionIdProvider;


/**
 * @author Isuru Weerarathna
 */
public class Rules extends AbstractExtensionId<RulesImpl> implements ExtensionIdProvider {

    public static final Rules RULES_PROVIDER = new Rules();

    private Rules() {}

    @Override
    public RulesImpl createExtension(ExtendedActorSystem system) {
        return new RulesImpl();
    }

    @Override
    public Rules lookup() {
        return Rules.RULES_PROVIDER;
    }
}
