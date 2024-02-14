/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import java.nio.file.Path
import java.sql.*

/**
 * ConnectionFactory that returns a new connection to an SQLite database every time getConnection() is called.
 */
class SqliteConnectionFactory(
    private val file: Path,
    tags: Set<String>
) : ConnectionFactory {

    // Take a copy of the passed in tags.
    private val tags = tags.toSet().onEach { require(supportedTableName(it)) { "unsupported tag: $it" } }

    private fun supportedTableName(tag: String): Boolean =
        when (tag) {
            IdIndex.ID_INDEX_TABLE -> false
            VERSION_TABLE -> false
            else -> tag.none { (!(Character.isLetterOrDigit(it) || it == '_')) }
        }

    @Throws(SQLException::class)
    override fun getConnection(): Connection =
        try {
            DriverManager.getConnection("jdbc:sqlite:file:$file?cache=shared").apply {
                validateSchema()
            }
        } catch (e: SQLException) {
            throw SQLException("Failed to initialise sqlite database '$file'", e)
        }

    private fun Connection.validateSchema() {
        try {
            createStatement().use { stmt ->
                checkVersion(stmt)

                ensureIdIndexTable(stmt)
                ensureMetadataTable(stmt)
                ensureBlobTables(stmt)
            }
        } catch (e: SQLException) {
            close()
            throw SQLException("Sqlite database '$file' failed schema validation", e)
        }
    }

    private fun checkVersion(stmt: Statement) {
        try {
            stmt.executeQuery("SELECT name FROM sqlite_master WHERE type='table' AND name='$VERSION_TABLE'").use { rs ->
                val tableExists = rs.next()
                if (!tableExists) {
                    stmt.executeUpdate("CREATE TABLE $VERSION_TABLE (version TEXT)")
                    stmt.execute("INSERT INTO $VERSION_TABLE (version) VALUES ($SCHEMA_VERSION)")
                }
            }
        } catch (e: SQLException) {
            throw SQLException("Failed to create version table", e)
        }

        var versionInDb: String? = null
        var matchingVersion = false
        stmt.executeQuery("SELECT * FROM $VERSION_TABLE").use { rs ->
            if (rs.next()) {
                versionInDb = rs.getString(1)
                matchingVersion = SCHEMA_VERSION == versionInDb
            }
        }

        if (!matchingVersion)
            throw SQLException("Wrong version number, expected $SCHEMA_VERSION found $versionInDb")
    }

    private fun ensureIdIndexTable(stmt: Statement) {
        try {
            stmt.execute("CREATE TABLE IF NOT EXISTS ${IdIndex.ID_INDEX_TABLE} (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT)")
        } catch (e: SQLException) {
            throw SQLException("Failed to create id index table", e)
        }

        try {
            stmt.execute("CREATE UNIQUE INDEX IF NOT EXISTS ${IdIndex.ID_INDEX_TABLE}_idx on ${IdIndex.ID_INDEX_TABLE} (entity_id)")
        } catch (e: SQLException) {
            throw SQLException("Failed to create index on id index table", e)
        }
    }

    private fun ensureMetadataTable(stmt: Statement) {
        try {
            stmt.execute("CREATE TABLE IF NOT EXISTS ${Metadata.TABLE_NAME} (key TEXT, value TEXT)")
        } catch (e: SQLException) {
            throw SQLException("Failed to create metadata table", e)
        }
    }

    private fun ensureBlobTables(stmt: Statement) {
        try {
            for (tag in tags) {
                stmt.execute("CREATE TABLE IF NOT EXISTS $tag (id INTEGER PRIMARY KEY, data BLOB) WITHOUT ROWID")
            }
        } catch (e: SQLException) {
            throw SQLException("Failed to create blob tables", e)
        }
    }

    companion object {
        const val VERSION_TABLE = "schema_version"
        const val SCHEMA_VERSION = "1"
    }

}
