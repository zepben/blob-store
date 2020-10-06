/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import com.zepben.annotations.EverythingIsNonnullByDefault;
import com.zepben.blobstore.BlobStore;
import com.zepben.blobstore.BlobStoreException;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Set;

/**
 * Blob store back by an SQLite database.
 * When using an instance of this class it uses a special {@link SqliteConnectionFactory} instance
 * that gives the same connection to both the reader and writer.
 */
@EverythingIsNonnullByDefault
public class SqliteBlobStore implements BlobStore {

    private final SqliteBlobReader reader;
    private final SqliteBlobWriter writer;
    @Nullable private Connection connection;

    @SuppressWarnings("WeakerAccess")
    public SqliteBlobStore(Path file, Set<String> tags) {
        // Wrap a connection factory instance so the reader and writer always share a connection.
        SqliteConnectionFactory factory = new SqliteConnectionFactory(file, tags);
        ConnectionFactory connectionFactory = () -> {
            if (connection == null) {
                connection = factory.getConnection();
                connection.setAutoCommit(false);
            }
            return connection;
        };

        writer = new SqliteBlobWriter(connectionFactory, tags);
        reader = new SqliteBlobReader(connectionFactory);
    }

    @Override
    public void close() throws BlobStoreException {
        writer.close();
        reader.close();
    }

    @Override
    public SqliteBlobReader reader() {
        return reader;
    }

    @Override
    public SqliteBlobWriter writer() {
        return writer;
    }

}
