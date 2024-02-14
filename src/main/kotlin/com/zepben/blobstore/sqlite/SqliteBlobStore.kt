/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import com.zepben.blobstore.BlobStore
import com.zepben.blobstore.BlobStoreException
import java.nio.file.Path
import java.sql.Connection

/**
 * Blob store back by an SQLite database.
 * When using an instance of this class it uses a special [SqliteConnectionFactory] instance
 * that gives the same connection to both the reader and writer.
 */
class SqliteBlobStore(file: Path, tags: Set<String>) : BlobStore {

    override val reader: SqliteBlobReader
    override val writer: SqliteBlobWriter

    private var connection: Connection? = null

    init {
        // Wrap a connection factory instance so the reader and writer always share a connection.
        val factory = SqliteConnectionFactory(file, tags)
        val connectionFactory = { connection ?: factory.getConnection().apply { autoCommit = false }.also { connection = it } }

        writer = SqliteBlobWriter(connectionFactory, tags)
        reader = SqliteBlobReader(connectionFactory)
    }

    @Throws(BlobStoreException::class)
    override fun close() {
        writer.close()
        reader.close()
    }

}
