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

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
    private static final String PASSWORD = "testPassword";
    private static final String DATABASE = "it";

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
        ObReaderConfig config = new ObReaderConfig();
        config.setRsList("127.0.0.1:2882:2881");
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);
        config.setStartTimestamp(0L);
        config.setTableWhiteList(TENANT + ".*.*");
        config.setWorkingMode("memory");

        ClientConf clientConf =
                ClientConf.builder()
                        .transferQueueSize(1000)
                        .connectTimeoutMs(3000)
                        .maxReconnectTimes(100)
                        .ignoreUnknownRecordType(true)
                        .build();

        LogProxyClient client = new LogProxyClient(LOG_PROXY.getHost(), 2983, config, clientConf);
        final CountDownLatch latch = new CountDownLatch(1);

        client.addListener(
                new RecordListener() {

                    boolean started = false;

                    @Override
                    public void notify(LogMessage message) {
                        if (!started) {
                            started = true;
                            latch.countDown();
                        }
                        switch (message.getOpt()) {
                            case INSERT:
                            case UPDATE:
                            case DELETE:
                                // note that the db name contains prefix '{tenant}.'
                                LOG.info(
                                        "Received log message of type {}: db: {}, table: {}, checkpoint {}",
                                        message.getOpt(),
                                        message.getDbName(),
                                        message.getTableName(),
                                        message.getCheckpoint());
                                // old fields for type 'UPDATE', 'DELETE'
                                LOG.info(
                                        "Old field values: {}",
                                        message.getFieldList().stream()
                                                .filter(DataMessage.Record.Field::isPrev)
                                                .collect(Collectors.toList()));
                                // new fields for type 'UPDATE', 'INSERT'
                                LOG.info(
                                        "New field values: {}",
                                        message.getFieldList().stream()
                                                .filter(field -> !field.isPrev())
                                                .collect(Collectors.toList()));
                                break;
                            case HEARTBEAT:
                                LOG.info(
                                        "Received heartbeat message with checkpoint {}",
                                        message.getCheckpoint());
                                break;
                            case BEGIN:
                            case COMMIT:
                                LOG.info("Received transaction message {}", message.getOpt());
                                break;
                            case DDL:
                                LOG.info(
                                        "Received log message with DDL: {}",
                                        message.getFieldList().get(0).getValue().toString());
                                break;
                            default:
                                throw new IllegalArgumentException(
                                        "Unsupported log message type: " + message.getOpt());
                        }
                    }

                    @Override
                    public void onException(LogProxyClientException e) {
                        LOG.error(e.getMessage());
                    }
                });
        client.start();

        if (!latch.await(30, TimeUnit.SECONDS)) {
            throw new TimeoutException("Timeout to receive messages in RecordListener");
        }

        try (Connection connection =
                        DriverManager.getConnection(OB_SERVER.getJdbcUrl(), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {

            statement.execute("USE " + DATABASE);
            statement.execute("CREATE TABLE user (id INT(10) PRIMARY KEY , name VARCHAR(20))");
            statement.execute("INSERT INTO user VALUES (1, 'wanghe') ");
            statement.execute("DROP TABLE user");
        }

        Thread.sleep(20_000);
        client.stop();
    }
}
