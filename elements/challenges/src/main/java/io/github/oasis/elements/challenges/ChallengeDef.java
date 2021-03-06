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

package io.github.oasis.elements.challenges;

import io.github.oasis.core.elements.AbstractDef;
import io.github.oasis.elements.challenges.spec.ChallengeSpecification;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Definition for challenge rule.
 *
 * This definition accepts two possible flags.
 *   - REPEATABLE_WINNERS: allows a single user to win the challenge multiple times.
 *   - OUT_OF_ORDER_WINNERS: allows processing out of order events and maintain constraints provided.
 *
 * @author Isuru Weerarathna
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ChallengeDef extends AbstractDef<ChallengeSpecification> {

}
