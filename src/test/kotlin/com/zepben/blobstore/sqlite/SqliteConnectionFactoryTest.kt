/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import com.zepben.testutils.exception.ExpectException.Companion.expect
import io.mockk.mockk
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.sql.SQLException

class SqliteConnectionFactoryTest {

    private var dbFile = Files.createTempFile(Path.of(System.getProperty("java.io.tmpdir")), "store.db", "")

    @AfterEach
    fun afterEach() {
        Files.deleteIfExists(dbFile)
    }

    @Test
    fun createsConnectionAndInitialisesDb() {
        val tags = setOf("tag1", "tag2")
        val factory = SqliteConnectionFactory(dbFile, tags)
        factory.getConnection().use { connection ->
            assertThat(connection, notNullValue())
            connection.createStatement().use { statement ->
                statement.executeQuery("select * from " + SqliteConnectionFactory.VERSION_TABLE).use { rs ->
                    assertThat("should have read the version record", rs.next())
                    assertThat(rs.getString(1), equalTo(SqliteConnectionFactory.SCHEMA_VERSION))
                }

                assertThat("should have created the table", statement.execute("select * from tag1"))
                assertThat("should have created the table", statement.execute("select * from tag2"))
                assertThat("should have created the table", statement.execute("select * from " + IdIndex.ID_INDEX_TABLE))
                assertThat(connection.metaData.url, containsString(dbFile.toString()))
            }
        }
    }

    @Test
    fun failsOnVersionMismatch() {
        val tags = setOf("tag1")
        val factory = SqliteConnectionFactory(dbFile, tags)
        factory.getConnection().use { connection ->
            assertThat(connection, notNullValue())
            connection.createStatement().use { statement ->
                statement.executeUpdate("update " + SqliteConnectionFactory.VERSION_TABLE + " set version = 'foo'")
            }
        }

        expect { SqliteConnectionFactory(dbFile, tags).getConnection() }
            .toThrow<SQLException>()
            .withMessage("Failed to initialise sqlite database '$dbFile'")
            .withCause<SQLException>()
    }

    @Test
    fun `can't use reserved names for tags`() {
        expect { SqliteConnectionFactory(mockk(), setOf(IdIndex.ID_INDEX_TABLE)) }
            .toThrow<IllegalArgumentException>()
            .withMessage("unsupported tag: ${IdIndex.ID_INDEX_TABLE}")

        expect { SqliteConnectionFactory(mockk(), setOf(SqliteConnectionFactory.VERSION_TABLE)) }
            .toThrow<IllegalArgumentException>()
            .withMessage("unsupported tag: ${SqliteConnectionFactory.VERSION_TABLE}")

        expect { SqliteConnectionFactory(mockk(), setOf("")) }
            .toThrow<IllegalArgumentException>()
            .withMessage("tags must not be empty strings")
    }

}
