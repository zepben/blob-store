/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import com.zepben.annotations.EverythingIsNonnullByDefault;
import com.zepben.blobstore.BlobReader;
import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.WhereBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

@EverythingIsNonnullByDefault
@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve"})
public class SqliteBlobReader implements BlobReader {

    // NOTE: Sqlite has a max parameterised query count of 999. Non scientific testing showed not much difference
    //       between 250 and higher numbers up to 999 so I just stuck with it.
    static final int MAX_PARAM_IDS_IN_QUERY = 250;

    private static final String MAX_IN_PARAMS_SQL = IntStream.range(0, MAX_PARAM_IDS_IN_QUERY)
        .mapToObj(i -> "?")
        .collect(joining(", ", " IN (", ")"));

    private final Logger logger = LoggerFactory.getLogger(SqliteBlobReader.class);

    private final ConnectionFactory connectionFactory;
    @Nullable private Connection connection;
    private final Map<Set<String>, PreparedStatement> cachedStatements = new HashMap<>();
    private final Metadata metadata;

    @SuppressWarnings("WeakerAccess")
    public SqliteBlobReader(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        metadata = new Metadata(this::getConnection);
    }

    @Override
    public void ids(Consumer<String> idHandler) {
        try (Statement stmt = getConnection().createStatement()) {
            try (ResultSet rs = stmt.executeQuery("select entity_id from " + IdIndex.ID_INDEX_TABLE)) {
                while (rs.next())
                    idHandler.accept(rs.getString(1));
            }
        } catch (SQLException e) {
            logAndNewException("failed to query ids", e, BlobStoreException::new);
        }
    }

    @Override
    public void ids(String tag, Consumer<String> idHandler) {
        try (Statement stmt = getConnection().createStatement()) {
            try (ResultSet rs = stmt.executeQuery("select entity_id from " + IdIndex.ID_INDEX_TABLE +
                " join " + tag + " on " + IdIndex.ID_INDEX_TABLE + ".id =" + tag + ".id")) {
                while (rs.next())
                    idHandler.accept(rs.getString(1));
            }
        } catch (SQLException e) {
            logAndNewException("failed to query ids", e, BlobStoreException::new);
        }
    }

    @Override
    public void forEach(Collection<String> ids,
                        Set<String> tags,
                        TagsHandler handler) throws BlobStoreException {
        if (!ids.isEmpty())
            doForEach(ids, tags, Collections.emptyList(), handler);
    }

    @Override
    public void forAll(Set<String> tags,
                       List<WhereBlob> whereBlobs,
                       TagsHandler handler) throws BlobStoreException {
        doForEach(Collections.emptyList(), tags, whereBlobs, handler);
    }

    @SuppressWarnings("WeakerAccess")
    @Nullable
    public String getMetadata(String key) throws BlobStoreException {
        try {
            return metadata.get(key);
        } catch (SQLException e) {
            throw new BlobStoreException("failed to read metadata for " + key, e);
        }
    }

    private Connection getConnection() throws SQLException {
        if (connection != null && connection.isClosed())
            throw new SQLException("reader connection has been closed");

        if (connection != null)
            return connection;

        connection = connectionFactory.getConnection();
        return connection;
    }

    @Override
    public void close() throws BlobStoreException {
        cachedStatements.values().forEach(statement -> {
            try {
                statement.close();
            } catch (SQLException e) {
                // If we can't close a statement the connection error handler will do.
            }
        });
        cachedStatements.clear();

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new BlobStoreException("failed to close reader connection", e);
        }
    }

    private void doForEach(Collection<String> ids,
                           Set<String> tags,
                           List<WhereBlob> whereBlobs,
                           TagsHandler handler) throws BlobStoreException {
        if (tags.isEmpty())
            return;

        try {
            int idsCount = 0;
            int idsSize = ids.size();
            Iterator<String> idIter = ids.iterator();

            // Build an array of the tags so the index order is fixed in the query
            String[] tagsArr = tags.toArray(new String[0]);

            do {
                int limitInQuery = Math.min(MAX_PARAM_IDS_IN_QUERY, idsSize - idsCount);
                idsCount += limitInQuery;

                PreparedStatement stmt;
                String sql;
                // We're no longer caching statements as we're using
                // a connection pool + driver caching
                sql = buildSql(tagsArr, whereBlobs, idsSize == 1 ? 1 : limitInQuery);
                stmt = getConnection().prepareStatement(sql);
                setStatementOptions(stmt);

                if (limitInQuery > 0)
                    prepareIds(stmt, idIter, limitInQuery);
                if (!whereBlobs.isEmpty())
                    prepareWheres(stmt, whereBlobs, limitInQuery);
                executeQuery(stmt, tagsArr, handler);

            } while (idsCount < idsSize);
        } catch (SQLException e) {
            throw logAndNewException("Error querying database", e, BlobStoreException::new);
        }
    }

    private void executeQuery(PreparedStatement stmt, String[] tags, TagsHandler handler) throws SQLException {
        Map<String, byte[]> blobs = new HashMap<>(tags.length);
        Map<String, byte[]> blobsView = Collections.unmodifiableMap(blobs);

        // when this is done, the statement is closed,
        // which will return the connection to the pool
        try (stmt; ResultSet rs = stmt.executeQuery()) {
            if (!rs.isClosed())
                rs.setFetchDirection(ResultSet.FETCH_FORWARD);

            while (rs.next()) {
                String id = rs.getString(1);
                for (int i = 0; i < tags.length; ++i) {
                    byte[] bytes = rs.getBytes(2 + i);
                    String tag = tags[i];
                    blobs.put(tag, bytes);
                }
                handler.handle(id, blobsView);
            }
        }
    }

    private void setStatementOptions(PreparedStatement statement) throws SQLException {
        statement.setFetchSize(MAX_PARAM_IDS_IN_QUERY);
        statement.setQueryTimeout(10);
    }

    private String buildSql(String[] tags, List<WhereBlob> wheres, int idInCount) {
        StringBuilder sql = new StringBuilder()
            .append("SELECT " + IdIndex.ID_INDEX_TABLE + ".entity_id, ")
            .append(Arrays.stream(tags).map(t -> t + ".data").collect(joining(",", "", " ")));

        if (tags.length == 1) {
            sql.append("FROM ").append(tags[0]).append(" JOIN ").append(IdIndex.ID_INDEX_TABLE).append(" ON ")
                .append(IdIndex.ID_INDEX_TABLE).append(".id = ").append(tags[0]).append(".id");
        } else {
            sql.append(" FROM " + IdIndex.ID_INDEX_TABLE);

            Set<String> whereTags = wheres.stream().map(WhereBlob::tag).collect(toSet());
            List<String> joins = new ArrayList<>();
            List<String> leftJoins = new ArrayList<>();
            for (String tag : tags) {
                if (whereTags.contains(tag))
                    joins.add(" JOIN " + tag + " on " + tag + ".id = " + IdIndex.ID_INDEX_TABLE + ".id");
                else
                    leftJoins.add(" LEFT JOIN " + tag + " on " + tag + ".id = " + IdIndex.ID_INDEX_TABLE + ".id");
            }

            sql.append(String.join(" ", joins));
            sql.append(String.join(" ", leftJoins));
        }

        if (idInCount > 0) {
            String inIds;
            if (idInCount == MAX_PARAM_IDS_IN_QUERY)
                inIds = MAX_IN_PARAMS_SQL;
            else
                inIds = IntStream.range(0, idInCount).mapToObj(i -> "?").collect(joining(", ", " IN (", ")"));

            sql.append(" WHERE " + IdIndex.ID_INDEX_TABLE).append(".entity_id ").append(inIds);
        }

        if (!wheres.isEmpty()) {
            if (idInCount == 0)
                sql.append(" WHERE ");
            else
                sql.append(" AND ");

            String whereBlobs = wheres.stream().map(w -> {
                String operator;
                switch (w.matchType()) {
                    case EQUAL:
                        operator = "=";
                        break;
                    case NOT_EQUAL:
                        operator = "<>";
                        break;
                    default:
                        throw new InternalError("Unhandled WhereBlob.Type switch case");
                }
                return w.tag() + ".data " + operator + " ?";
            }).collect(joining(" AND"));
            sql.append(whereBlobs);
        }

        return sql.toString();
    }

    private void prepareIds(PreparedStatement stmt, Iterator<String> iter, int limit) throws SQLException {
        int index = 0;
        while (iter.hasNext() && index < limit) {
            stmt.setString(++index, iter.next());
        }
    }

    private void prepareWheres(PreparedStatement stmt, List<WhereBlob> whereBlobs, int idInCount) throws SQLException {
        for (WhereBlob whereBlob : whereBlobs) {
            stmt.setBytes(++idInCount, whereBlob.blob());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private <T extends Throwable> T logAndNewException(String msg,
                                                       @Nullable Throwable t,
                                                       BiFunction<String, Throwable, T> newException) {
        logger.debug(msg);
        return newException.apply(msg, t);
    }

}
