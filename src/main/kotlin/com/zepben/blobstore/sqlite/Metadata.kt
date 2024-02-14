/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import java.sql.Connection
import java.sql.SQLException

class Metadata(
    private val getConnection: () -> Connection
) {

    @Throws(SQLException::class)
    fun write(key: String, value: String): Boolean {
        getConnection().prepareStatement(INSERT_SQL).use { stmt ->
            stmt.setString(1, key)
            stmt.setString(2, value)
            return stmt.executeUpdate() > 0
        }
    }

    @Throws(SQLException::class)
    fun update(key: String, value: String): Boolean {
        getConnection().prepareStatement(UPDATE_SQL).use { stmt ->
            stmt.setString(2, key)
            stmt.setString(1, value)
            return stmt.executeUpdate() > 0
        }
    }

    @Throws(SQLException::class)
    fun delete(key: String): Boolean {
        getConnection().prepareStatement(DELETE_SQL).use { stmt ->
            stmt.setString(1, key)
            return stmt.executeUpdate() > 0
        }
    }

    @Throws(SQLException::class)
    operator fun get(key: String): String? {
        getConnection().prepareStatement(SELECT_SQL).use { stmt ->
            stmt.setString(1, key)
            stmt.executeQuery().use { rs -> if (rs.next()) return rs.getString(1) }
        }
        return null
    }

    companion object {

        const val TABLE_NAME = "metadata"

        private const val INSERT_SQL = "INSERT INTO $TABLE_NAME (key, value) VALUES (?, ?)"
        private const val UPDATE_SQL = "UPDATE $TABLE_NAME SET value = ? WHERE key = ?"
        private const val DELETE_SQL = "DELETE FROM $TABLE_NAME WHERE key = ?"
        private const val SELECT_SQL = "SELECT value FROM $TABLE_NAME WHERE key = ?"

    }

}
