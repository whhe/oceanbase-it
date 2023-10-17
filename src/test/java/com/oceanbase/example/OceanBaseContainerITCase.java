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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

public class OceanBaseContainerITCase {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseContainerITCase.class);

    private static final String SYS_PASSWORD = "password";
    private static final String TENANT = "test";
    private static final String USERNAME = "root@" + TENANT;
    private static final String PASSWORD = "123456";

    @ClassRule
    public static final OceanBaseContainer OB_SERVER =
            new OceanBaseContainer("oceanbase/oceanbase-ce:4.2.0.0")
                    .withNetworkMode("host")
                    .withEnv("MODE", "slim")
                    .withEnv("OB_ROOT_PASSWORD", SYS_PASSWORD)
                    .withEnv("OB_TENANT_NAME", TENANT)
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("init.sql"),
                            "/root/boot/init.d/init.sql")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final GenericContainer<?> LOG_PROXY =
            new GenericContainer<>("whhe/oblogproxy:1.1.3_4x")
                    .withNetworkMode("host")
                    .withEnv("OB_SYS_USERNAME", "root")
                    .withEnv("OB_SYS_PASSWORD", SYS_PASSWORD)
                    .waitingFor(Wait.forLogMessage(".*boot success!.*", 1))
                    .withStartupTimeout(Duration.ofMinutes(1))
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(OB_SERVER, LOG_PROXY)).join();
        LOG.info("Containers are started.");

        try (Connection connection =
                        DriverManager.getConnection(OB_SERVER.getJdbcUrl(), USERNAME, "");
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("ALTER USER root IDENTIFIED BY '%s'", PASSWORD));
        } catch (SQLException e) {
            LOG.error("Failed to set password for test user", e);
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        Stream.of(OB_SERVER, LOG_PROXY).forEach(GenericContainer::stop);
        LOG.info("Containers are stopped.");
    }

    @Test
    public void testOceanBaseCDC() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // env.getConfig()
        // env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");
        //        env.getCheckpointConfig().setCheckpointStorage("file:///Users/boleyn/checkpoint");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        // tenv.getConfig().getConfiguration().setString("table.exec.sink.not-null-enforcer",
        // "DROP");

        tenv.executeSql(
                "CREATE TABLE products (\n"
                        + "    rowid INT,\n"
                        + "    name STRING,\n"
                        + "    description STRING,\n"
                        + "    PRIMARY KEY (rowid) NOT ENFORCED\n"
                        + "  ) WITH (\n"
                        + "    'connector' = 'oceanbase-cdc',\n"
                        + "    'scan.startup.mode' = 'initial',\n"
                        + "    'username' = 'root@test',\n"
                        + "    'password' = '123456',\n"
                        + "    'tenant-name' = 'test',\n"
                        + "    'table-list' = 'test.products',\n"
                        + "    'hostname' = '127.0.0.1',\n"
                        + "    'port' = '2881',\n"
                        + "    'rootserver-list' = '127.0.0.1:2882:2881',\n"
                        + "    'logproxy.host' = '127.0.0.1',\n"
                        + "    'logproxy.port' = '2983',\n"
                        + "    'working-mode' = 'memory'\n"
                        + " )");

        tenv.executeSql("select * from products").print();
    }
}
