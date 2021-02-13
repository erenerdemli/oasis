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

package io.github.oasis.core.services.api.dao;

import io.github.oasis.core.elements.AttributeInfo;
import io.github.oasis.core.elements.ElementDef;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;

/**
 * @author Isuru Weerarathna
 */
public interface IElementDao {

    @SqlUpdate
    @GetGeneratedKeys("id")
    int insertNewElement(int gameId, ElementDef elementDef);

    @SqlQuery
    ElementDef readElement(int rowId);

    @SqlQuery
    ElementDef readElementById(String elementId);

    @SqlUpdate
    void updateElement(String id, ElementDef update);

    @SqlUpdate
    void deleteElementById(String elementId);


    @SqlUpdate
    int insertAttribute(int gameId, AttributeInfo newAttr);

    @SqlQuery
    AttributeInfo readAttribute(int gameId, int id);

    @SqlQuery
    List<AttributeInfo> readAllAttributes(int gameId);

}
