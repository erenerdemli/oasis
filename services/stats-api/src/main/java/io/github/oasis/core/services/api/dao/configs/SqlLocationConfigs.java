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

package io.github.oasis.core.services.api.dao.configs;

/**
 * @author Isuru Weerarathna
 */

import org.jdbi.v3.core.config.JdbiConfig;

/**
 * @author Isuru Weerarathna
 */
public class SqlLocationConfigs implements JdbiConfig<SqlLocationConfigs> {

    private String sqlScriptPath;

    public SqlLocationConfigs() {
        this.sqlScriptPath = "";
    }

    private SqlLocationConfigs(SqlLocationConfigs other) {
        this.sqlScriptPath = other.sqlScriptPath;
    }

    public String getSqlScriptPath() {
        return sqlScriptPath;
    }

    public void setSqlScriptPath(String sqlScriptPath) {
        this.sqlScriptPath = sqlScriptPath;
    }

    @Override
    public SqlLocationConfigs createCopy() {
        return new SqlLocationConfigs(this);
    }
}