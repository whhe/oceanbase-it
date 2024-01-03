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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.MountableFile;

public class OceanBaseContainerITCase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseContainerITCase.class);

    private static final String SYS_PASSWORD = "password";
    private static final String TENANT = "test";

    @ClassRule
    public static final OceanBaseContainer OB_SERVER =
            new OceanBaseContainer("oceanbase/oceanbase-ce:4.2.1_bp3")
                    .withNetworkMode("host")
                    .withEnv("MODE", "slim")
                    .withEnv("OB_ROOT_PASSWORD", SYS_PASSWORD)
                    .withEnv("OB_TENANT_NAME", TENANT)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("init.sql"),
                            "/root/boot/init.d/init.sql")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @Test
    public void testJDBC() throws Exception {
        while (true) {
            if (connected()) {
                LOG.info("Connected successfully.");
                break;
            }
        }
    }

    private boolean connected() {
        try (Connection connection =
                        DriverManager.getConnection("jdbc:mysql://127.0.0.1:2881", "root@test", "");
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("select version()");
            return rs.next();
        } catch (SQLException e) {
            // do nothing
        }
        return false;
    }
}
