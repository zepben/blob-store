/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.zepben.blobstore.sqlite.SqliteConnectionFactory.SCHEMA_VERSION;
import static com.zepben.blobstore.sqlite.SqliteConnectionFactory.VERSION_TABLE;
import static com.zepben.testutils.exception.ExpectException.expect;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqliteConnectionFactoryTest {

    private static final String tempDB = "store.db";
    private static Path dbFile;

    static {
        try {
            dbFile = Files.createTempFile(Path.of("/tmp"), tempDB, "");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    public static void afterAll() throws IOException {
        Files.deleteIfExists(dbFile);
    }

    @BeforeEach
    public void before() throws IOException {
        Files.deleteIfExists(dbFile);
        dbFile = Files.createTempFile(Path.of("/tmp"), tempDB, "");
    }

    @Test
    @SuppressWarnings("SqlResolve")
    public void createsConnectionAndInitialisesDb() throws Exception {
        Set<String> tags = new HashSet<>(Arrays.asList("tag1", "tag2"));
        SqliteConnectionFactory factory = new SqliteConnectionFactory(dbFile, tags);
        try (Connection connection = factory.getConnection()) {
            assertNotNull(connection);

            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery("select * from " + VERSION_TABLE);
                assertTrue(rs.next());
                assertThat(rs.getString(1), equalTo(SCHEMA_VERSION));

                boolean isResultSet = statement.execute("select * from tag1");
                assertThat(isResultSet, is(true));

                isResultSet = statement.execute("select * from tag2");
                assertThat(isResultSet, is(true));

                isResultSet = statement.execute("select * from " + IdIndex.ID_INDEX_TABLE);
                assertThat(isResultSet, is(true));

                assertThat(connection.getMetaData().getURL(), containsString(dbFile.toString()));
            }
        }
    }

    @Test
    @SuppressWarnings("SqlResolve")
    public void failsOnVersionMismatch() throws Exception {
        Set<String> tags = new HashSet<>(Collections.singletonList("tag1"));
        SqliteConnectionFactory factory = new SqliteConnectionFactory(dbFile, tags);
        try (Connection connection = factory.getConnection()) {
            assertNotNull(connection);
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("update " + VERSION_TABLE + " set version = 'foo'");
            }
        }

        expect(() -> new SqliteConnectionFactory(dbFile, tags).getConnection())
                .toThrow(SQLException.class)
                .withMessage("Failed to initialise sqlite database '" + dbFile.toString() + "'");

    }

}
