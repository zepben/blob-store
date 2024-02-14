/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import com.zepben.blobstore.BlobReader
import com.zepben.blobstore.BlobStoreException
import com.zepben.blobstore.TagsHandler
import com.zepben.blobstore.WhereBlob
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*

class SqliteBlobReader(
    private val connectionFactory: ConnectionFactory
) : BlobReader {

    private val logger = LoggerFactory.getLogger(javaClass)
    private var connection: Connection? = null
    private val metadata = Metadata(::getConnection)

    override fun ids(idHandler: (String) -> Unit) {
        try {
            getConnection().createStatement().use { stmt ->
                stmt.executeQuery("select entity_id from " + IdIndex.ID_INDEX_TABLE).use { rs ->
                    while (rs.next())
                        idHandler(rs.getString(1))
                }
            }
        } catch (e: SQLException) {
            throw logAndNewException("failed to query ids", e)
        }
    }

    override fun ids(tag: String, idHandler: (String) -> Unit) {
        try {
            getConnection().createStatement().use { stmt ->
                stmt.executeQuery("select entity_id from ${IdIndex.ID_INDEX_TABLE} join $tag on ${IdIndex.ID_INDEX_TABLE}.id = $tag.id").use { rs ->
                    while (rs.next())
                        idHandler(rs.getString(1))
                }
            }
        } catch (e: SQLException) {
            throw logAndNewException("failed to query ids", e)
        }
    }

    @Throws(BlobStoreException::class)
    override fun forEach(ids: Collection<String>, tags: Set<String>, handler: TagsHandler) {
        if (!ids.isEmpty())
            doForEach(ids, tags, emptyList(), handler)
    }

    @Throws(BlobStoreException::class)
    override fun forAll(tags: Set<String>, whereBlobs: List<WhereBlob>, handler: TagsHandler) {
        doForEach(emptyList(), tags, whereBlobs, handler)
    }

    @Throws(BlobStoreException::class)
    fun getMetadata(key: String): String? =
        try {
            metadata[key]
        } catch (e: SQLException) {
            throw BlobStoreException("failed to read metadata for $key", e)
        }

    @Throws(BlobStoreException::class)
    override fun close() {
        try {
            connection?.close()
        } catch (e: SQLException) {
            throw BlobStoreException("failed to close reader connection", e)
        }
    }

    private fun getConnection(): Connection {
        if (connection?.isClosed == true)
            throw SQLException("reader connection has been closed")

        return connection ?: connectionFactory.getConnection().also { connection = it }
    }

    private fun doForEach(ids: Collection<String>, tags: Set<String>, whereBlobs: List<WhereBlob>, handler: TagsHandler) {
        if (tags.isEmpty())
            return

        try {
            var idsCount = 0
            val idsSize = ids.size
            val idIter = ids.iterator()

            // Build an array of the tags so the index order is fixed in the query
            val tagsArr = tags.toTypedArray()
            do {
                val limitInQuery = MAX_PARAM_IDS_IN_QUERY.coerceAtMost(idsSize - idsCount)
                idsCount += limitInQuery

                val sql = buildSql(tagsArr, whereBlobs, limitInQuery)
                getConnection().prepareStatement(sql).use { stmt ->
                    setStatementOptions(stmt)

                    if (limitInQuery > 0)
                        prepareIds(stmt, idIter, limitInQuery)
                    if (whereBlobs.isNotEmpty())
                        prepareWheres(stmt, whereBlobs, limitInQuery)

                    executeQuery(stmt, tagsArr, handler)
                }
            } while (idsCount < idsSize)
        } catch (e: SQLException) {
            throw logAndNewException("Error querying database", e)
        }
    }

    private fun executeQuery(stmt: PreparedStatement, tags: Array<String>, handler: TagsHandler) {
        val blobs = mutableMapOf<String, ByteArray?>()
        val blobsView = Collections.unmodifiableMap(blobs)

        stmt.executeQuery().use { rs ->
            if (!rs.isClosed)
                rs.fetchDirection = ResultSet.FETCH_FORWARD

            while (rs.next()) {
                val id = rs.getString(1)
                tags.forEachIndexed { i, tag ->
                    blobs[tag] = rs.getBytes(2 + i)
                }

                handler(id, blobsView)
            }
        }
    }

    private fun setStatementOptions(statement: PreparedStatement) {
        statement.fetchSize = MAX_PARAM_IDS_IN_QUERY
        statement.queryTimeout = 10
    }

    private fun buildSql(tags: Array<String>, wheres: List<WhereBlob>, idInCount: Int): String =
        buildString {
            append("SELECT ${IdIndex.ID_INDEX_TABLE}.entity_id, ")
            append(tags.joinToString(",", "", " ") { "$it.data" })

            if (tags.size == 1) {
                append("FROM ${tags[0]} JOIN ${IdIndex.ID_INDEX_TABLE} ON ${IdIndex.ID_INDEX_TABLE}.id = ${tags[0]}.id")
            } else {
                append(" FROM ${IdIndex.ID_INDEX_TABLE}")

                val whereTags = wheres.map { it.tag }.toSet()
                val (joins, leftJoins) = tags.partition { whereTags.contains(it) }

                append(joins.joinToString(" ") { tag ->
                    " JOIN $tag on $tag.id = ${IdIndex.ID_INDEX_TABLE}.id"
                })
                append(leftJoins.joinToString(" ") { tag ->
                    " LEFT JOIN $tag on $tag.id = ${IdIndex.ID_INDEX_TABLE}.id"
                })
            }

            var wherePrefix = " WHERE "
            if (idInCount > 0) {
                val inIds = if (idInCount == MAX_PARAM_IDS_IN_QUERY)
                    MAX_IN_PARAMS_SQL
                else
                    (0..<idInCount).joinToString(", ", " IN (", ")") { "?" }

                append(" WHERE ${IdIndex.ID_INDEX_TABLE}.entity_id $inIds")
                wherePrefix = " AND "
            }

            if (wheres.isNotEmpty()) {
                val whereBlobs = wheres.joinToString(" AND ", wherePrefix) { w ->
                    "${w.tag}.data ${w.matchType.operator} ?"
                }
                append(whereBlobs)
            }
        }

    private fun prepareIds(stmt: PreparedStatement, iter: Iterator<String>, limit: Int) {
        var index = 0
        while (iter.hasNext() && (index < limit)) {
            stmt.setString(++index, iter.next())
        }
    }

    private fun prepareWheres(stmt: PreparedStatement, whereBlobs: List<WhereBlob>, limit: Int) {
        // We start the index at the limit as the ids take up the first [limit] parameters.
        var index = limit
        whereBlobs.forEach {
            stmt.setBytes(++index, it.blob)
        }
    }

    private fun logAndNewException(msg: String, t: Throwable?): BlobStoreException {
        logger.debug(msg)
        return BlobStoreException(msg, t)
    }

    companion object {

        // NOTE: Sqlite has a max parameterised query count of 999. Non-scientific testing showed not much difference
        //       between 250 and higher numbers up to 999, so I just stuck with it.
        const val MAX_PARAM_IDS_IN_QUERY = 250

        private val MAX_IN_PARAMS_SQL = (0..<MAX_PARAM_IDS_IN_QUERY)
            .joinToString(separator = ", ", prefix = " IN (", postfix = ")") { "?" }

    }

}
