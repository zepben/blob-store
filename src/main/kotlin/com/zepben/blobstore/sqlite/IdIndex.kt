/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException

class IdIndex(
    private val getConnection: () -> Connection
) : AutoCloseable {

    private val insertIdIndexStmt = LazyPreparedStatement(INSERT_ID_INDEX_SQL)
    private val selectIdIndexStmt = LazyPreparedStatement(SELECT_ID_INDEX_SQL)
    private val deleteIdIndexStmt = LazyPreparedStatement(DELETE_ID_INDEX_SQL)

    @Throws(SQLException::class)
    fun getIndexId(id: String): Int {
        try {
            selectIdIndexStmt.getStatement().apply {
                setString(1, id)
                executeQuery().use { rs -> return if (!rs.next()) -1 else rs.getInt(1) }
            }
        } catch (e: SQLException) {
            close()
            throw e
        }
    }

    @Throws(SQLException::class)
    fun createIndexId(id: String): Boolean =
        try {
            insertIdIndexStmt.getStatement().run {
                setString(1, id)
                executeUpdate() > 0
            }
        } catch (e: SQLException) {
            close()
            throw e
        }

    @Throws(SQLException::class)
    fun deleteIndexId(indexId: Int): Boolean =
        try {
            deleteIdIndexStmt.getStatement().run {
                setInt(1, indexId)
                executeUpdate() > 0
            }
        } catch (e: SQLException) {
            close()
            throw e
        }

    @Throws(SQLException::class)
    override fun close() {
        insertIdIndexStmt.close()
        selectIdIndexStmt.close()
        deleteIdIndexStmt.close()
    }

    private inner class LazyPreparedStatement(private val stmtSql: String) : AutoCloseable {

        private var statement: PreparedStatement? = null

        @Throws(SQLException::class)
        fun getStatement(): PreparedStatement =
            statement?.takeUnless { it.isClosed }
                ?: getConnection().prepareStatement(stmtSql).also { statement = it }

        @Throws(SQLException::class)
        override fun close() {
            statement?.close()
            statement = null
        }
    }

    companion object {

        const val ID_INDEX_TABLE = "entity_ids"

        private const val INSERT_ID_INDEX_SQL = "insert into $ID_INDEX_TABLE (entity_id) VALUES (?)"
        private const val SELECT_ID_INDEX_SQL = "select rowid from $ID_INDEX_TABLE where entity_id = ?"
        private const val DELETE_ID_INDEX_SQL = "delete from $ID_INDEX_TABLE where id = ?"

    }

}
