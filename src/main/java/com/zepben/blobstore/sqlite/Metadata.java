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
class Metadata {

    static final String TABLE_NAME = "metadata";
    private static final String INSERT_SQL = String.format("INSERT INTO %s (key, value) VALUES (?, ?)", TABLE_NAME);
    private static final String UPDATE_SQL = String.format("UPDATE %s SET value = ? WHERE key = ?", TABLE_NAME);
    private static final String DELETE_SQL = String.format("DELETE FROM %s WHERE key = ?", TABLE_NAME);
    private static final String SELECT_SQL = String.format("SELECT value FROM %s WHERE key = ?", TABLE_NAME);

    private final ConnectionSupplier connectionSupplier;

    Metadata(ConnectionSupplier connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    boolean write(String key, String value) throws SQLException {
        try (PreparedStatement stmt = connectionSupplier.get().prepareStatement(INSERT_SQL)) {
            stmt.setString(1, key);
            stmt.setString(2, value);
            return stmt.executeUpdate() > 0;
        }
    }

    boolean update(String key, String value) throws SQLException {
        try (PreparedStatement stmt = connectionSupplier.get().prepareStatement(UPDATE_SQL)) {
            stmt.setString(2, key);
            stmt.setString(1, value);
            return stmt.executeUpdate() > 0;
        }
    }

    boolean delete(String key) throws SQLException {
        try (PreparedStatement stmt = connectionSupplier.get().prepareStatement(DELETE_SQL)) {
            stmt.setString(1, key);
            return stmt.executeUpdate() > 0;
        }
    }

    @Nullable
    String get(String key) throws SQLException {
        try (PreparedStatement stmt = connectionSupplier.get().prepareStatement(SELECT_SQL)) {
            stmt.setString(1, key);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next())
                    return rs.getString(1);
            }
        }
        return null;
    }

}
