/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.example;

import java.time.Duration;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class OceanBaseContainer extends JdbcDatabaseContainer<OceanBaseContainer> {

    private static final Integer SQL_PORT = 2881;

    public OceanBaseContainer(String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public OceanBaseContainer(DockerImageName dockerImageName) {
        super(dockerImageName);

        this.waitStrategy =
                Wait.forLogMessage(".*boot success!.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3));
    }

    @Override
    public String getDriverClassName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        Integer port = "host".equals(getNetworkMode()) ? SQL_PORT : getMappedPort(SQL_PORT);
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:mysql://" + getHost() + ":" + port + "/test" + additionalUrlParams;
    }

    @Override
    public String getDatabaseName() {
        return "test";
    }

    @Override
    public String getUsername() {
        return "root@test";
    }

    @Override
    public String getPassword() {
        return "";
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }
}
