/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import com.zepben.annotations.EverythingIsNonnullByDefault;
import com.zepben.blobstore.BlobReadWriteException;
import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.BlobWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

@EverythingIsNonnullByDefault
@SuppressWarnings({"SqlNoDataSourceInspection"})
public class SqliteBlobWriter implements BlobWriter {

    private final Logger logger = LoggerFactory.getLogger(SqliteBlobWriter.class);

    private static final String SELECT_ID_FROM_INDEX = "(select id from " + IdIndex.ID_INDEX_TABLE + " where entity_id = ?)";
    private static final String INSERT_SQL_FORMAT = "insert into %s (id, data) values (" + SELECT_ID_FROM_INDEX + ", ?)";
    private static final String INSERT_IGNORE_SQL_FORMAT = "insert or ignore into %s (id, data) values (" + SELECT_ID_FROM_INDEX + ", ?)";
    private static final String UPDATE_SQL_FORMAT = "update %s set data = ? where id = " + SELECT_ID_FROM_INDEX;
    private static final String DELETE_SQL_FORMAT = "delete from %s where id = " + SELECT_ID_FROM_INDEX;

    private final IdIndex idIndex;
    private final Metadata metadata;
    private final PreparedStatementCache updateStatements = new PreparedStatementCache(UPDATE_SQL_FORMAT);
    private final PreparedStatementCache insertStatements = new PreparedStatementCache(INSERT_SQL_FORMAT);
    private final PreparedStatementCache insertIgnoreStatements = new PreparedStatementCache(INSERT_IGNORE_SQL_FORMAT);
    private final PreparedStatementCache deleteStatements = new PreparedStatementCache(DELETE_SQL_FORMAT);

    private final ConnectionFactory connectionFactory;
    private final Set<String> tags;
    @Nullable private Connection connection = null;

    @SuppressWarnings("WeakerAccess")
    public SqliteBlobWriter(ConnectionFactory connectionFactory, Set<String> tags) {
        this.connectionFactory = connectionFactory;
        this.tags = new HashSet<>(tags);
        idIndex = new IdIndex(this::getConnection);
        metadata = new Metadata(this::getConnection);
    }

    private Connection getConnection() throws SQLException {
        if (connection != null && connection.isClosed())
            throw new SQLException("writer connection has been closed");

        if (connection != null)
            return connection;

        connection = connectionFactory.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    // This is here because of a bug in SQLite where once an exception is thrown, all prepared statements seem to become invalid.
    private void resetPreparedStatements() throws BlobStoreException {
        try {
            idIndex.close();
            insertStatements.close();
            updateStatements.close();
            insertIgnoreStatements.close();
            deleteStatements.close();
        } catch (SQLException e) {
            throw logAndNewException(
                "Failed to reset prepared statements. Queries will no longer work",
                e,
                BlobStoreException::new);
        }
    }

    @Override
    public boolean write(String id, String tag, byte[] buffer, int offset, int length) throws BlobStoreException {
        validateTag(tag);

        try {
            // Try to insert
            PreparedStatement insertStmt = insertIgnoreStatements.getStatement(tag);
            if (doInsert(id, buffer, offset, length, insertStmt))
                return true;

            // It might of failed due to no id in the index table, create index and try again
            idIndex.createIndexId(id);
            insertStmt = insertStatements.getStatement(tag);
            return doInsert(id, buffer, offset, length, insertStmt);
        } catch (SQLException e) {
            resetPreparedStatements();
            throw logAndNewException(
                "Failed to insert item " + id,
                e,
                (msg, t) -> new BlobReadWriteException(id, msg, t));
        }
    }

    private boolean doInsert(String id, byte[] buffer, int offset, int length, PreparedStatement insertStmt) throws SQLException {
        insertStmt.setString(1, id);
        if (offset == 0 && buffer.length == length) {
            insertStmt.setBytes(2, buffer);
        } else {
            insertStmt.setBinaryStream(2, new ByteArrayBackedInputStream(buffer, offset, length), length);
        }
        return insertStmt.executeUpdate() > 0;
    }

    @Override
    public boolean update(String id, String tag, byte[] buffer, int offset, int length) throws BlobStoreException {
        validateTag(tag);

        try {
            PreparedStatement updateStmt = updateStatements.getStatement(tag);
            updateStmt.setString(2, id);
            if (offset == 0 && buffer.length == length) {
                updateStmt.setBytes(1, buffer);
            } else {
                updateStmt.setBinaryStream(1, new ByteArrayBackedInputStream(buffer, offset, length), length);
            }

            return updateStmt.executeUpdate() > 0;
        } catch (SQLException e) {
            resetPreparedStatements();
            throw logAndNewException(
                "Failed to update item " + id,
                e,
                (msg, t) -> new BlobReadWriteException(id, msg, t));
        }
    }

    @Override
    public boolean delete(String id) throws BlobStoreException {
        try {
            int indexId = idIndex.getIndexId(id);
            if (indexId == -1)
                return false;

            for (String tag : tags) {
                deleteById(id, tag);
            }

            return idIndex.deleteIndexId(indexId);
        } catch (SQLException e) {
            resetPreparedStatements();
            throw logAndNewException(
                "Failed to delete id " + id,
                e,
                (msg, t) -> new BlobReadWriteException(id, msg, t));
        }
    }

    @Override
    public boolean delete(String id, String tag) throws BlobStoreException {
        validateTag(tag);

        try {
            return deleteById(id, tag);
        } catch (SQLException e) {
            resetPreparedStatements();
            throw logAndNewException(
                "Failed to delete item " + id,
                e,
                (msg, t) -> new BlobReadWriteException(id, msg, t));
        }
    }

    @SuppressWarnings("WeakerAccess")
    public boolean writeMetadata(String key, String value) throws BlobStoreException {
        try {
            return metadata.write(key, value);
        } catch (SQLException e) {
            resetPreparedStatements();
            throw logAndNewException(
                "Failed to write metadata for " + key,
                e,
                (msg, t) -> new BlobReadWriteException(key, msg, t));
        }
    }

    @SuppressWarnings("WeakerAccess")
    public boolean updateMetadata(String key, String value) throws BlobStoreException {
        try {
            return metadata.update(key, value);
        } catch (SQLException e) {
            resetPreparedStatements();
            throw logAndNewException(
                "Failed to update metadata for " + key,
                e,
                (msg, t) -> new BlobReadWriteException(key, msg, t));
        }
    }

    @SuppressWarnings("WeakerAccess")
    public boolean deleteMetadata(String key) throws BlobStoreException {
        try {
            return metadata.delete(key);
        } catch (SQLException e) {
            resetPreparedStatements();
            throw logAndNewException(
                "Failed to delete metadata for " + key,
                e,
                (msg, t) -> new BlobReadWriteException(key, msg, t));
        }
    }

    private boolean deleteById(String id, String tag) throws SQLException {
        PreparedStatement deleteStmt = deleteStatements.getStatement(tag);
        deleteStmt.setString(1, id);
        return deleteStmt.executeUpdate() > 0;
    }

    @Override
    public void commit() throws BlobStoreException {
        try {
            getConnection().commit();
        } catch (SQLException e) {
            resetPreparedStatements();
            throw logAndNewException("Failed to commit", e, BlobStoreException::new);
        }
    }

    @Override
    public void rollback() throws BlobStoreException {
        try {
            getConnection().rollback();
        } catch (SQLException e) {
            resetPreparedStatements();
            throw logAndNewException("Failed to rollback", e, BlobStoreException::new);
        }
    }

    private void validateTag(String tag) {
        if (!tags.contains(tag))
            throw new IllegalArgumentException("unsupported tag: " + tag);
    }

    @Override
    public void close() throws BlobStoreException {
        try {
            if (connection != null && !connection.isClosed()) {
                idIndex.close();
                insertStatements.close();
                updateStatements.close();
                insertIgnoreStatements.close();
                deleteStatements.close();
                connection.close();
            }
        } catch (SQLException e) {
            throw new BlobStoreException("failed to close writer", e);
        }
    }

    private <T extends BlobStoreException> T logAndNewException(String msg,
                                                                @Nullable Throwable t,
                                                                BiFunction<String, Throwable, T> newException) {
        logger.debug(msg);
        return newException.apply(msg, t);
    }

    @EverythingIsNonnullByDefault
    private class PreparedStatementCache {

        private final String statementSql;
        private final Map<String, PreparedStatement> statements = new HashMap<>();

        PreparedStatementCache(String statementSql) {
            this.statementSql = statementSql;
        }

        PreparedStatement getStatement(String tag) throws SQLException {
            PreparedStatement statement = statements.get(tag);
            if (statement == null || statement.isClosed()) {
                statement = getConnection().prepareStatement(String.format(statementSql, tag));
                statements.put(tag, statement);
            }
            return statement;
        }

        void close() throws SQLException {
            for (PreparedStatement statement : statements.values()) {
                statement.close();
            }
            statements.clear();
        }

    }

}
