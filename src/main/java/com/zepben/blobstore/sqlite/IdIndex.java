/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@EverythingIsNonnullByDefault
class IdIndex implements AutoCloseable {

    static final String ID_INDEX_TABLE = "entity_ids";
    private static final String INSERT_ID_INDEX_SQL = String.format("insert into %s (entity_id) VALUES (?)", ID_INDEX_TABLE);
    private static final String SELECT_ID_INDEX_SQL = String.format("select rowid from %s where entity_id = ?", ID_INDEX_TABLE);
    private static final String DELETE_ID_INDEX_SQL = String.format("delete from %s where id = ?", ID_INDEX_TABLE);

    private final ConnectionSupplier connectionSupplier;
    private LazyPreparedStatement insertIdIndexStmt = new LazyPreparedStatement(INSERT_ID_INDEX_SQL);
    private LazyPreparedStatement selectIdIndexStmt = new LazyPreparedStatement(SELECT_ID_INDEX_SQL);
    private LazyPreparedStatement deleteIdIndexStmt = new LazyPreparedStatement(DELETE_ID_INDEX_SQL);

    IdIndex(ConnectionSupplier connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    int getIndexId(String id) throws SQLException {
        try {
            PreparedStatement stmt = selectIdIndexStmt.getStatement();
            stmt.setString(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next())
                    return -1;
                else
                    return rs.getInt(1);
            }
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    void createIndexId(String id) throws SQLException {
        try {
            PreparedStatement stmt = insertIdIndexStmt.getStatement();
            stmt.setString(1, id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    boolean deleteIndexId(int indexId) throws SQLException {
        try {
            PreparedStatement stmt = deleteIdIndexStmt.getStatement();
            stmt.setInt(1, indexId);
            return stmt.executeUpdate() > 0;
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    @Override
    public void close() throws SQLException {
        insertIdIndexStmt.close();
        selectIdIndexStmt.close();
        deleteIdIndexStmt.close();
    }

    @EverythingIsNonnullByDefault
    private class LazyPreparedStatement {

        private final String stmtSql;
        @Nullable private PreparedStatement statement = null;

        LazyPreparedStatement(String stmtSql) {
            this.stmtSql = stmtSql;
        }

        PreparedStatement getStatement() throws SQLException {
            if (statement == null || statement.isClosed())
                statement = connectionSupplier.get().prepareStatement(stmtSql);
            return statement;
        }

        void close() throws SQLException {
            if (statement != null) {
                statement.close();
                statement = null;
            }
        }

    }

}
