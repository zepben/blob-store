/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import com.zepben.blobstore.BlobReadWriteException
import com.zepben.blobstore.BlobStoreException
import com.zepben.blobstore.BlobWriter
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException

class SqliteBlobWriter(
    private val connectionFactory: ConnectionFactory,
    tags: Set<String>
) : BlobWriter {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val idIndex = IdIndex(::getConnection)
    private val metadata = Metadata(::getConnection)

    private val updateStatements = PreparedStatementCache(UPDATE_SQL_FORMAT)
    private val insertStatements = PreparedStatementCache(INSERT_SQL_FORMAT)
    private val insertIgnoreStatements = PreparedStatementCache(INSERT_IGNORE_SQL_FORMAT)
    private val deleteStatements = PreparedStatementCache(DELETE_SQL_FORMAT)

    // Take a copy of the tags set.
    private val tags = tags.toSet()
    private var connection: Connection? = null

    @Throws(BlobStoreException::class)
    override fun write(id: String, tag: String, buffer: ByteArray, offset: Int, length: Int): Boolean {
        validateTag(tag)

        return try {
            // Try to insert
            var insertStmt = insertIgnoreStatements.getStatement(tag)
            if (doInsert(id, buffer, offset, length, insertStmt))
                return true

            // It might have failed due to no id in the index table, create index and try again
            idIndex.createIndexId(id)
            insertStmt = insertStatements.getStatement(tag)
            doInsert(id, buffer, offset, length, insertStmt)
        } catch (e: SQLException) {
            resetPreparedStatements()
            throw logAndNewException("Failed to insert item $id", e) { msg, t -> BlobReadWriteException(id, msg, t) }
        }
    }

    @Throws(BlobStoreException::class)
    override fun update(id: String, tag: String, buffer: ByteArray, offset: Int, length: Int): Boolean {
        validateTag(tag)

        return try {
            val updateStmt = updateStatements.getStatement(tag)
            updateStmt.setString(2, id)

            if ((offset == 0) && (buffer.size == length)) {
                updateStmt.setBytes(1, buffer)
            } else {
                updateStmt.setBinaryStream(1, ByteArrayBackedInputStream(buffer, offset, length), length)
            }

            updateStmt.executeUpdate() > 0
        } catch (e: SQLException) {
            resetPreparedStatements()
            throw logAndNewException("Failed to update item $id", e) { msg, t -> BlobReadWriteException(id, msg, t) }
        }
    }

    @Throws(BlobStoreException::class)
    override fun delete(id: String): Boolean {
        return try {
            val indexId = idIndex.getIndexId(id)
            if (indexId == -1)
                return false

            tags.forEach { deleteById(id, it) }

            idIndex.deleteIndexId(indexId)
        } catch (e: SQLException) {
            resetPreparedStatements()
            throw logAndNewException("Failed to delete id $id", e) { msg, t -> BlobReadWriteException(id, msg, t) }
        }
    }

    @Throws(BlobStoreException::class)
    override fun delete(id: String, tag: String): Boolean {
        validateTag(tag)

        return try {
            deleteById(id, tag)
        } catch (e: SQLException) {
            resetPreparedStatements()
            throw logAndNewException("Failed to delete item $id", e) { msg, t -> BlobReadWriteException(id, msg, t) }
        }
    }

    @Throws(BlobStoreException::class)
    override fun commit() {
        try {
            getConnection().commit()
        } catch (e: SQLException) {
            resetPreparedStatements()
            throw logAndNewException("Failed to commit", e) { message, cause -> BlobStoreException(message, cause) }
        }
    }

    @Throws(BlobStoreException::class)
    override fun rollback() {
        try {
            getConnection().rollback()
        } catch (e: SQLException) {
            resetPreparedStatements()
            throw logAndNewException("Failed to rollback", e) { message, cause -> BlobStoreException(message, cause) }
        }
    }

    @Throws(BlobStoreException::class)
    override fun close() {
        try {
            connection?.takeUnless { it.isClosed }?.also {
                idIndex.close()
                insertStatements.close()
                updateStatements.close()
                insertIgnoreStatements.close()
                deleteStatements.close()

                it.close()
            }
        } catch (e: SQLException) {
            throw BlobStoreException("failed to close writer", e)
        }
    }

    @Throws(BlobStoreException::class)
    fun writeMetadata(key: String, value: String): Boolean =
        try {
            metadata.write(key, value)
        } catch (e: SQLException) {
            resetPreparedStatements()
            throw logAndNewException("Failed to write metadata for $key", e) { msg, t -> BlobReadWriteException(key, msg, t) }
        }

    @Throws(BlobStoreException::class)
    fun updateMetadata(key: String, value: String): Boolean =
        try {
            metadata.update(key, value)
        } catch (e: SQLException) {
            resetPreparedStatements()
            throw logAndNewException("Failed to update metadata for $key", e) { msg, t -> BlobReadWriteException(key, msg, t) }
        }

    @Throws(BlobStoreException::class)
    fun deleteMetadata(key: String): Boolean =
        try {
            metadata.delete(key)
        } catch (e: SQLException) {
            resetPreparedStatements()
            throw logAndNewException("Failed to delete metadata for $key", e) { msg, t -> BlobReadWriteException(key, msg, t) }
        }

    private fun getConnection(): Connection {
        if (connection?.isClosed == true)
            throw SQLException("writer connection has been closed")

        return connection ?: connectionFactory.getConnection().apply { autoCommit = false }.also { connection = it }
    }

    // This is here because of a bug in SQLite where once an exception is thrown, all prepared statements seem to become invalid.
    private fun resetPreparedStatements() {
        try {
            idIndex.close()
            insertStatements.close()
            updateStatements.close()
            insertIgnoreStatements.close()
            deleteStatements.close()
        } catch (e: SQLException) {
            throw logAndNewException("Failed to reset prepared statements. Queries will no longer work", e, ::BlobStoreException)
        }
    }

    private fun doInsert(id: String, buffer: ByteArray, offset: Int, length: Int, insertStmt: PreparedStatement): Boolean {
        insertStmt.setString(1, id)

        if ((offset == 0) && (buffer.size == length))
            insertStmt.setBytes(2, buffer)
        else
            insertStmt.setBinaryStream(2, ByteArrayBackedInputStream(buffer, offset, length), length)

        return insertStmt.executeUpdate() > 0
    }

    private fun deleteById(id: String, tag: String): Boolean =
        deleteStatements.getStatement(tag).run {
            setString(1, id)
            executeUpdate() > 0
        }

    private fun validateTag(tag: String) {
        require(tags.contains(tag)) { "unsupported tag: $tag" }
    }

    private fun <T : BlobStoreException> logAndNewException(msg: String, t: Throwable?, newException: (String, Throwable?) -> T): T =
        newException(msg, t).apply {
            logger.debug(msg)
        }

    private inner class PreparedStatementCache(private val statementSql: String) {

        private val statements: MutableMap<String, PreparedStatement> = HashMap()

        @Throws(SQLException::class)
        fun getStatement(tag: String): PreparedStatement =
            statements[tag]?.takeUnless { it.isClosed }
                ?: getConnection().prepareStatement(String.format(statementSql, tag)).also { statements[tag] = it }

        @Throws(SQLException::class)
        fun close() {
            statements.values.forEach { it.close() }
            statements.clear()
        }

    }

    companion object {

        private const val SELECT_ID_FROM_INDEX = "(select id from ${IdIndex.ID_INDEX_TABLE} where entity_id = ?)"
        private const val INSERT_SQL_FORMAT = "insert into %s (id, data) values ($SELECT_ID_FROM_INDEX, ?)"
        private const val INSERT_IGNORE_SQL_FORMAT = "insert or ignore into %s (id, data) values ($SELECT_ID_FROM_INDEX, ?)"
        private const val UPDATE_SQL_FORMAT = "update %s set data = ? where id = $SELECT_ID_FROM_INDEX"
        private const val DELETE_SQL_FORMAT = "delete from %s where id = $SELECT_ID_FROM_INDEX"

    }

}
