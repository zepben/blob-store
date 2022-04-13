/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zepben.annotations.EverythingIsNonnullByDefault;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * ConnectionFactory that returns a new connection to an SQLite database every time getConnection() is called.
 */
@EverythingIsNonnullByDefault
public class SqliteConnectionFactory implements ConnectionFactory {

    static final String VERSION_TABLE = "schema_version";
    static final String SCHEMA_VERSION = "1";

    private final Set<String> tags;
    private final Path file;
    private final HikariDataSource dbSource;

    @SuppressWarnings("WeakerAccess")
    public SqliteConnectionFactory(Path file, Set<String> tags) {
        for (String tag : tags) {
            if (!supportedTableName(tag))
                throw new IllegalArgumentException("unsupported tag: " + tag);
        }

        this.tags = Set.copyOf(tags);
        this.file = file;
        this.dbSource = buildDbSource("jdbc:sqlite:file:" + file + "?cache=shared");
    }

    private boolean supportedTableName(String tag) {
        if (tag.equals(IdIndex.ID_INDEX_TABLE))
            return false;

        if (tag.equals(VERSION_TABLE))
            return false;

        for (int i = 0, n = tag.length(); i < n; ++i) {
            char c = tag.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_'))
                return false;
        }
        return true;
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = null;
        try {
            connection = dbSource.getConnection();

            checkVersion(connection);
            createTables(connection);

            return connection;
        } catch (SQLException e) {
            if (connection != null)
                connection.close();

            throw new SQLException(String.format("Failed to initialise sqlite database '%s'", file.toString()), e);
        }
    }

    private void checkVersion(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            try {
                String sql = String.format("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'", VERSION_TABLE);
                boolean tableExists = stmt.executeQuery(sql).next();
                if (!tableExists) {
                    sql = String.format("CREATE TABLE %s (version TEXT)", VERSION_TABLE);
                    stmt.executeUpdate(sql);

                    sql = String.format("INSERT INTO %s (version) VALUES (%s)", VERSION_TABLE, SCHEMA_VERSION);
                    stmt.execute(sql);
                }
            } catch (SQLException e) {
                throw new SQLException("Failed to create version table", e);
            }

            String versionInDb = null;
            boolean matchingVersion = false;
            String sql = String.format("SELECT * FROM %s", VERSION_TABLE);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                versionInDb = rs.getString(1);
                matchingVersion = SCHEMA_VERSION.equals(versionInDb);
            }

            if (!matchingVersion) {
                String msg = String.format("Wrong version number, expected %s found %s", SCHEMA_VERSION, versionInDb);
                throw new SQLException(msg);
            }
        }
    }

    private void createTables(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            createIdIndexTable(stmt);
            createMetadataTable(stmt);
            createBlobTables(stmt);
        }
    }

    private void createIdIndexTable(Statement stmt) throws SQLException {
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT)", IdIndex.ID_INDEX_TABLE);
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new SQLException("Failed to create id index table", e);
        }

        sql = String.format("CREATE UNIQUE INDEX IF NOT EXISTS %s_idx on %<s (entity_id)", IdIndex.ID_INDEX_TABLE);
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new SQLException("Failed to create index on id index table", e);
        }
    }

    private void createMetadataTable(Statement stmt) throws SQLException {
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (key TEXT, value TEXT)", Metadata.TABLE_NAME);
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new SQLException("Failed to create metadata table", e);
        }
    }

    private void createBlobTables(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY, data BLOB) WITHOUT ROWID";
        try {
            for (String tag : tags) {
                stmt.execute(String.format(sql, tag));
            }
        } catch (SQLException e) {
            throw new SQLException("Failed to create blob tables", e);
        }
    }

    private HikariDataSource buildDbSource(String url) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        // these are arbitrary and not scientific
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        // TODO: we don't need more than 1 connection in the pool atm; will add more if needed; potentially in the
        // thread that works with the DB (not load-profiles)
        config.setMaximumPoolSize(1);

        return new HikariDataSource(config);
    }
}
