/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import com.zepben.blobstore.BlobReadWriteException;
import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.WhereBlob;
import kotlin.Unit;
import kotlin.jvm.functions.Function3;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.zepben.testutils.exception.ExpectException.expect;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("SqlNoDataSourceInspection")
public class SqliteBlobStoreTest {

    private static Path dbFile;

    @BeforeEach
    public void before() throws IOException {
        if (dbFile != null) Files.deleteIfExists(dbFile);
        dbFile = Files.createTempFile(Path.of(System.getProperty("java.io.tmpdir")), "store.db", "");
    }

    @AfterAll
    public static void afterAll() throws IOException {
        Files.deleteIfExists(dbFile);
    }

    private Set<String> tagsSet(String... tags) {
        return new HashSet<>(Arrays.asList(tags));
    }

    private byte[] bytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; ++i)
            bytes[i] = (byte) values[i];
        return bytes;
    }

    @Test
    public void rejectsInvalidTags() {
        expect(() -> new SqliteBlobStore(dbFile, tagsSet("alpha_numerics_123", "not;allowed")))
            .toThrow(IllegalArgumentException.class);
    }

    @Test
    public void storeUpdateDelete() throws Exception {
        Set<String> tags = tagsSet("tag1", "tag2");
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tags)) {
            String item1 = "a";
            String item2 = "b";
            String item3 = "c";
            byte[] bytes1 = bytes(1);
            byte[] bytes2 = bytes(2);
            byte[] bytes3 = bytes(3, 4, 5, 6);
            byte[] bytes3Expected = bytes(4, 5);
            assertTrue(store.getWriter().write(item1, "tag1", bytes1));
            assertTrue(store.getWriter().write(item2, "tag1", bytes2));
            assertTrue(store.getWriter().write(item3, "tag1", bytes3, 1, 2));
            store.getWriter().commit();

            Map<String, byte[]> items = store.getReader().getAll("tag1");
            assertThat(items.get(item1), is(bytes1));
            assertThat(items.get(item2), is(bytes2));
            assertThat(items.get(item3), is(bytes3Expected));

            items = store.getReader().get(Arrays.asList(item2, item3), "tag1");
            assertThat(items.get(item2), is(bytes2));
            assertThat(items.get(item3), is(bytes3Expected));

            byte[] b1Update = bytes(1, 1, 1, 1);
            byte[] b1UpdateExpected = bytes(1, 1, 1);
            assertTrue(store.getWriter().update(item1, "tag1", b1Update, 1, 3));
            store.getWriter().commit();

            byte[] bytes = store.getReader().get(item1, "tag1");
            assertNotNull(bytes);
            assertThat(bytes, equalTo(b1UpdateExpected));

            store.getWriter().write(item1, "tag2", bytes3);
            store.getWriter().commit();
            assertTrue(store.getWriter().delete(item1));
            store.getWriter().commit();
            items = store.getReader().getAll("tag1");
            assertFalse(items.containsKey(item1));
            items = store.getReader().getAll("tag2");
            assertFalse(items.containsKey(item1));

            store.getWriter().write(item2, "tag2", bytes3);
            store.getWriter().commit();
            assertTrue(store.getWriter().delete(item2, "tag1"));
            store.getWriter().commit();
            final CountDownLatch latch = new CountDownLatch(1);
            store.getReader().forEach(Collections.singleton(item2), tags, (id, blobs) -> {
                assertThat(blobs.keySet(), containsInAnyOrder("tag1", "tag2"));
                assertThat(blobs.get("tag1"), equalTo(null));
                assertThat(blobs.get("tag2"), equalTo(bytes3));
                latch.countDown();
                return null;
            });

            boolean receivedSignal = latch.await(1, TimeUnit.SECONDS);
            assertTrue(receivedSignal);
        }
    }

    @Test
    public void getMultiTags() throws BlobStoreException {
        Set<String> tags = tagsSet("tag1", "tag2");
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tags)) {
            byte[] a1 = bytes(1);
            byte[] a2 = bytes(1, 1);
            byte[] b = bytes(1);
            byte[] c = bytes(1);

            store.getWriter().write("a", "tag1", a1);
            store.getWriter().write("b", "tag1", b);
            store.getWriter().write("a", "tag2", a2);
            store.getWriter().write("c", "tag2", c);
            store.getWriter().commit();

            AtomicBoolean calledBack = new AtomicBoolean(false);
            store.getReader().forEach(Arrays.asList("a", "b", "c"), tags, (id, blobs) -> {
                calledBack.set(true);
                assertThat(blobs.keySet(), containsInAnyOrder("tag1", "tag2"));
                switch (id) {
                    case "a":
                        assertThat(blobs.get("tag1"), equalTo(a1));
                        assertThat(blobs.get("tag2"), equalTo(a2));
                        break;
                    case "b":
                        assertThat(blobs.get("tag1"), equalTo(b));
                        assertThat(blobs.get("tag2"), equalTo(null));
                        break;
                    case "c":
                        assertThat(blobs.get("tag1"), equalTo(null));
                        assertThat(blobs.get("tag2"), equalTo(c));
                        break;
                    default:
                        fail("unexpected id: " + id);
                }
                return null;
            });
            assertTrue(calledBack.get());
        }
    }

    @Test
    public void missingTagsThrows() {
        expect(() -> {
                try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag"))) {
                    Map<String, Function3<String, String, byte[], Unit>> tags = new HashMap<>();
                    tags.put("missing", (id, tag, bytes) -> null);
                    store.getReader().forAll(tagsSet("missing"), ((id, blobs) -> null));
                }
            }
        ).toThrow(BlobStoreException.class);
    }

    @Test
    public void forAllWhere() throws Exception {
        Set<String> tags = tagsSet("tag1");
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tags)) {
            byte[] b1 = bytes(1, 2);
            byte[] b2 = bytes(3, 4);
            store.getWriter().write("id1", "tag1", b1);
            store.getWriter().write("id2", "tag1", b2);

            List<String> matched = new ArrayList<>();
            WhereBlob whereBlob = WhereBlob.Companion.equals("tag1", b1);
            store.getReader().forAll(tags, Collections.singletonList(whereBlob), (id, blobs) -> {
                matched.add(id);
                return null;
            });
            assertThat(matched, contains("id1"));

            matched.clear();
            whereBlob = WhereBlob.Companion.notEqual("tag1", b1);
            store.getReader().forAll(tags, Collections.singletonList(whereBlob), (id, blobs) -> {
                matched.add(id);
                return null;
            });
            assertThat(matched, contains("id2"));
        }
    }

    @Test
    public void rollsback() throws BlobStoreException {
        final String tag = "tests";
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet(tag))) {
            byte[] b1 = bytes(1);
            store.getWriter().write("a", tag, b1);
            store.getWriter().commit();

            byte[] readBytes = store.getReader().get("a", tag);
            assertThat(readBytes, equalTo(b1));

            byte[] b1Update = bytes(1, 1);
            store.getWriter().update("a", tag, b1Update);
            store.getWriter().rollback();

            readBytes = store.getReader().get("a", tag);
            assertThat(readBytes, equalTo(b1));
        }
    }

    @Test
    public void metadata() throws Exception {
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet())) {
            assertTrue(store.getWriter().writeMetadata("test", "value1"));
            String value = store.getReader().getMetadata("test");
            assertThat(value, equalTo("value1"));

            assertTrue(store.getWriter().updateMetadata("test", "value2"));
            value = store.getReader().getMetadata("test");
            assertThat(value, equalTo("value2"));

            assertTrue(store.getWriter().deleteMetadata("test"));
            value = store.getReader().getMetadata("test");
            assertThat(value, equalTo(null));
        }
    }

    @Test
    public void writeThrowsOnIdViolation() {
        final String tag = "tests";
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet(tag))) {
                byte[] b1 = bytes(1);
                try {
                    store.getWriter().write("a", tag, b1);
                    store.getWriter().write("a", tag, b1);
                } catch (Exception ex) {
                    assertThat(((BlobReadWriteException) ex).getItemId(), equalTo("a"));
                    assertThat(ex.getCause(), instanceOf(SQLException.class));
                    throw ex;
                }
            }
        }).toThrow(BlobReadWriteException.class);
    }

    // The SQLite driver has a bug where if an exception is thrown all existing prepared statements become invalid.
    // This test should test all prepared statements again after an exception is thrown
    @Test
    public void resetsPreparedStatementsAfterException() throws Exception {
        final String tag = "tests";
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet(tag))) {
            byte[] b1 = bytes(1);
            try {
                // Force db to throw an exception
                store.getWriter().write("a", tag, b1);
                store.getWriter().write("a", tag, b1);
            } catch (Exception ex) {
                assertTrue(store.getWriter().write("b", tag, b1));
                assertTrue(store.getWriter().update("b", tag, b1));
                assertTrue(store.getWriter().delete("b"));
                store.getWriter().commit();
                store.getWriter().rollback();
            }
        }
    }

    @Test
    public void deleteRejectsIncorrectId() throws BlobStoreException {
        final String tag = "tests";
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet(tag))) {
            boolean validId = store.getWriter().delete("a", tag);
            assertThat(validId, is(false));
        }
    }

    @Test
    public void deleteRejectsOnMultipleDeleteForSameId() throws Exception {
        final String tag = "tests";
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet(tag))) {
            byte[] b1 = bytes(1);
            store.getWriter().write("a", tag, b1);
            store.getWriter().delete("a", tag);
            boolean validId = store.getWriter().delete("a", tag);
            assertThat(validId, is(false));
        }
    }

    @Test
    public void getsIds() throws BlobStoreException {
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag1", "tag2"))) {
            byte[] bytes = bytes(1);
            store.getWriter().write("id1", "tag1", bytes);
            store.getWriter().write("id2", "tag2", bytes);
            store.getWriter().write("idboth", "tag1", bytes);
            store.getWriter().write("idboth", "tag2", bytes);

            Set<String> ids = store.getReader().ids();
            assertThat(ids, containsInAnyOrder("id1", "id2", "idboth"));
        }
    }

    @Test
    public void getsIdsWithTag() throws BlobStoreException {
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag1", "tag2"))) {
            byte[] bytes = bytes(1);
            store.getWriter().write("id1", "tag1", bytes);
            store.getWriter().write("id2", "tag2", bytes);
            store.getWriter().write("idboth", "tag1", bytes);
            store.getWriter().write("idboth", "tag2", bytes);

            Set<String> ids = store.getReader().ids("tag1");
            assertThat(ids, containsInAnyOrder("id1", "idboth"));

            ids = store.getReader().ids("tag2");
            assertThat(ids, containsInAnyOrder("id2", "idboth"));
        }
    }

    //TODO: Test that gets more than 1000 IDs to validate split queries works

    @Test
    public void forEachWithEmptyIdsDoesNothing() throws BlobStoreException {
        Set<String> tags = tagsSet("tag1", "tag2");
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tags)) {
            AtomicInteger counter = new AtomicInteger(0);
            store.getReader().forEach(Collections.emptyList(), tags, (id, blobs) -> {
                counter.incrementAndGet();
                return null;
            });
            assertThat(counter.get(), equalTo(0));
        }
    }

    @Test
    public void forEachWithEmptyTagsDoesNothing() throws BlobStoreException {
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag1", "tag2"))) {
            AtomicInteger counter = new AtomicInteger(0);
            store.getReader().forEach(Collections.singletonList("id"), Collections.emptySet(), (id, blobs) -> {
                counter.incrementAndGet();
                return null;
            });
            assertThat(counter.get(), equalTo(0));
        }
    }

    @Test
    public void writeFailsWithUnknownTag() {
        final String tag = "tests";
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, Collections.singleton(tag))) {
                byte[] bytes = bytes(1);
                store.getWriter().write("a", "unknown", bytes);
            }
        }).toThrow(IllegalArgumentException.class);
    }

    @Test
    public void updateFailsWithUnknownTag() {
        final String tag = "tests";
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, Collections.singleton(tag))) {
                byte[] bytes = bytes(1);
                store.getWriter().update("a", "unknown", bytes);
            }
        }).toThrow(IllegalArgumentException.class);
    }

    @Test
    public void deleteFailsWithUnknownTag() {
        final String tag = "tests";
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, Collections.singleton(tag))) {
                byte[] bytes = bytes(1);
                store.getWriter().write("a", tag, bytes);
                store.getWriter().delete("a", "unknown");
            }
        }).toThrow(IllegalArgumentException.class);
    }

    @Test
    public void forEachFailsWithUnknownKey() {
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag"))) {
                store.getReader().forEach(Collections.singleton("a"), "unknown", ((id, tag, bytes) -> null));
            }
        }).toThrow(BlobStoreException.class);
    }

    @Test
    public void forAllManyFailsWithUnknownKey() {
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag"))) {
                store.getReader().forAll("unknown", ((id, tag, bytes) -> null));
            }
        }).toThrow(BlobStoreException.class);
    }

    @Test
    public void handlesLargestSqliteParamCountQuery() throws Exception {
        Set<String> ids = IntStream.range(0, SqliteBlobReader.MAX_PARAM_IDS_IN_QUERY + 1).mapToObj(Integer::toString).collect(toSet());
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag"))) {
            store.getReader().forEach(ids, "tag", ((id, tag, bytes) -> null));
        }
    }

}
