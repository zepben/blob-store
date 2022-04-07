/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import com.zepben.blobstore.BlobReadWriteException;
import com.zepben.blobstore.BlobReader;
import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.WhereBlob;
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

    private static final String tempDB = "store.db";
    private static Path dbFile;

    static {
        try {
            dbFile = Files.createTempFile(Path.of("/tmp"), tempDB, "");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    public void before() throws IOException {
        Files.deleteIfExists(dbFile);
        dbFile = Files.createTempFile(Path.of("/tmp"), tempDB, "");
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
            assertTrue(store.writer().write(item1, "tag1", bytes1));
            assertTrue(store.writer().write(item2, "tag1", bytes2));
            assertTrue(store.writer().write(item3, "tag1", bytes3, 1, 2));
            store.writer().commit();

            Map<String, byte[]> items = store.reader().getAll("tag1");
            assertThat(items.get(item1), is(bytes1));
            assertThat(items.get(item2), is(bytes2));
            assertThat(items.get(item3), is(bytes3Expected));

            items = store.reader().get(Arrays.asList(item2, item3), "tag1");
            assertThat(items.get(item2), is(bytes2));
            assertThat(items.get(item3), is(bytes3Expected));

            byte[] b1Update = bytes(1, 1, 1, 1);
            byte[] b1UpdateExpected = bytes(1, 1, 1);
            assertTrue(store.writer().update(item1, "tag1", b1Update, 1, 3));
            store.writer().commit();

            byte[] bytes = store.reader().get(item1, "tag1");
            assertNotNull(bytes);
            assertThat(bytes, equalTo(b1UpdateExpected));

            store.writer().write(item1, "tag2", bytes3);
            store.writer().commit();
            assertTrue(store.writer().delete(item1));
            store.writer().commit();
            items = store.reader().getAll("tag1");
            assertFalse(items.containsKey(item1));
            items = store.reader().getAll("tag2");
            assertFalse(items.containsKey(item1));

            store.writer().write(item2, "tag2", bytes3);
            store.writer().commit();
            assertTrue(store.writer().delete(item2, "tag1"));
            store.writer().commit();
            final CountDownLatch latch = new CountDownLatch(1);
            store.reader().forEach(Collections.singleton(item2), tags, (id, blobs) -> {
                assertThat(blobs.keySet(), containsInAnyOrder("tag1", "tag2"));
                assertThat(blobs.get("tag1"), equalTo(null));
                assertThat(blobs.get("tag2"), equalTo(bytes3));
                latch.countDown();
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

            store.writer().write("a", "tag1", a1);
            store.writer().write("b", "tag1", b);
            store.writer().write("a", "tag2", a2);
            store.writer().write("c", "tag2", c);
            store.writer().commit();

            AtomicBoolean calledBack = new AtomicBoolean(false);
            store.reader().forEach(Arrays.asList("a", "b", "c"), tags, (id, blobs) -> {
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
            });
            assertTrue(calledBack.get());
        }
    }

    @Test
    public void missingTagsThrows() {
        expect(() -> {
                    try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag"))) {
                        Map<String, BlobReader.BlobHandler> tags = new HashMap<>();
                        tags.put("missing", (id, tag, bytes) -> {
                        });
                        store.reader().forAll(tagsSet("missing"), ((id, blobs) -> {
                        }));
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
            store.writer().write("id1", "tag1", b1);
            store.writer().write("id2", "tag1", b2);

            List<String> matched = new ArrayList<>();
            WhereBlob whereBlob = WhereBlob.equals("tag1", b1);
            store.reader().forAll(tags, Collections.singletonList(whereBlob), (id, blobs) -> matched.add(id));
            assertThat(matched, contains("id1"));

            matched.clear();
            whereBlob = WhereBlob.notEqual("tag1", b1);
            store.reader().forAll(tags, Collections.singletonList(whereBlob), (id, blobs) -> matched.add(id));
            assertThat(matched, contains("id2"));
        }
    }

    @Test
    public void rollsback() throws BlobStoreException {
        final String tag = "tests";
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet(tag))) {
            byte[] b1 = bytes(1);
            store.writer().write("a", tag, b1);
            store.writer().commit();

            byte[] readBytes = store.reader().get("a", tag);
            assertThat(readBytes, equalTo(b1));

            byte[] b1Update = bytes(1, 1);
            store.writer().update("a", tag, b1Update);
            store.writer().rollback();

            readBytes = store.reader().get("a", tag);
            assertThat(readBytes, equalTo(b1));
        }
    }

    @Test
    public void metadata() throws Exception {
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet())) {
            assertTrue(store.writer().writeMetadata("test", "value1"));
            String value = store.reader().getMetadata("test");
            assertThat(value, equalTo("value1"));

            assertTrue(store.writer().updateMetadata("test", "value2"));
            value = store.reader().getMetadata("test");
            assertThat(value, equalTo("value2"));

            assertTrue(store.writer().deleteMetadata("test"));
            value = store.reader().getMetadata("test");
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
                    store.writer().write("a", tag, b1);
                    store.writer().write("a", tag, b1);
                } catch (BlobReadWriteException ex) {
                    assertThat(ex.itemId(), equalTo("a"));
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
                store.writer().write("a", tag, b1);
                store.writer().write("a", tag, b1);
            } catch (BlobReadWriteException ex) {
                assertTrue(store.writer().write("b", tag, b1));
                assertTrue(store.writer().update("b", tag, b1));
                assertTrue(store.writer().delete("b"));
                store.writer().commit();
                store.writer().rollback();
            }
        }
    }

    @Test
    public void deleteRejectsIncorrectId() throws BlobStoreException {
        final String tag = "tests";
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet(tag))) {
            boolean validId = store.writer().delete("a", tag);
            assertThat(validId, is(false));
        }
    }

    @Test
    public void deleteRejectsOnMultipleDeleteForSameId() throws Exception {
        final String tag = "tests";
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet(tag))) {
            byte[] b1 = bytes(1);
            store.writer().write("a", tag, b1);
            store.writer().delete("a", tag);
            boolean validId = store.writer().delete("a", tag);
            assertThat(validId, is(false));
        }
    }

    @Test
    public void getsIds() throws BlobStoreException {
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag1", "tag2"))) {
            byte[] bytes = bytes(1);
            store.writer().write("id1", "tag1", bytes);
            store.writer().write("id2", "tag2", bytes);
            store.writer().write("idboth", "tag1", bytes);
            store.writer().write("idboth", "tag2", bytes);

            Set<String> ids = store.reader().ids();
            assertThat(ids, containsInAnyOrder("id1", "id2", "idboth"));
        }
    }

    @Test
    public void getsIdsWithTag() throws BlobStoreException {
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag1", "tag2"))) {
            byte[] bytes = bytes(1);
            store.writer().write("id1", "tag1", bytes);
            store.writer().write("id2", "tag2", bytes);
            store.writer().write("idboth", "tag1", bytes);
            store.writer().write("idboth", "tag2", bytes);

            Set<String> ids = store.reader().ids("tag1");
            assertThat(ids, containsInAnyOrder("id1", "idboth"));

            ids = store.reader().ids("tag2");
            assertThat(ids, containsInAnyOrder("id2", "idboth"));
        }
    }

    //TODO: Test that gets more than 1000 IDs to validate split queries works

    @Test
    public void forEachWithEmptyIdsDoesNothing() throws BlobStoreException {
        Set<String> tags = tagsSet("tag1", "tag2");
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tags)) {
            AtomicInteger counter = new AtomicInteger(0);
            store.reader().forEach(Collections.emptyList(), tags, (id, blobs) -> counter.incrementAndGet());
            assertThat(counter.get(), equalTo(0));
        }
    }

    @Test
    public void forEachWithEmptyTagsDoesNothing() throws BlobStoreException {
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag1", "tag2"))) {
            AtomicInteger counter = new AtomicInteger(0);
            store.reader().forEach(Collections.singletonList("id"), Collections.emptySet(), (id, blobs) -> counter.incrementAndGet());
            assertThat(counter.get(), equalTo(0));
        }
    }

    @Test
    public void writeFailsWithUnknownTag() {
        final String tag = "tests";
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, Collections.singleton(tag))) {
                byte[] bytes = bytes(1);
                store.writer().write("a", "unknown", bytes);
            }
        }).toThrow(IllegalArgumentException.class);
    }

    @Test
    public void updateFailsWithUnknownTag() {
        final String tag = "tests";
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, Collections.singleton(tag))) {
                byte[] bytes = bytes(1);
                store.writer().update("a", "unknown", bytes);
            }
        }).toThrow(IllegalArgumentException.class);
    }

    @Test
    public void deleteFailsWithUnknownTag() {
        final String tag = "tests";
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, Collections.singleton(tag))) {
                byte[] bytes = bytes(1);
                store.writer().write("a", tag, bytes);
                store.writer().delete("a", "unknown");
            }
        }).toThrow(IllegalArgumentException.class);
    }

    @Test
    public void forEachFailsWithUnknownKey() {
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag"))) {
                store.reader().forEach(Collections.singleton("a"), "unknown", ((id, tag, bytes) -> {
                }));
            }
        }).toThrow(BlobStoreException.class);
    }

    @Test
    public void forAllManyFailsWithUnknownKey() {
        expect(() -> {
            try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag"))) {
                store.reader().forAll("unknown", ((id, tag, bytes) -> {
                }));
            }
        }).toThrow(BlobStoreException.class);
    }

    @Test
    public void handlesLargestSqliteParamCountQuery() throws Exception {
        Set<String> ids = IntStream.range(0, SqliteBlobReader.MAX_PARAM_IDS_IN_QUERY + 1).mapToObj(Integer::toString).collect(toSet());
        try (SqliteBlobStore store = new SqliteBlobStore(dbFile, tagsSet("tag"))) {
            store.reader().forEach(ids, "tag", ((id, tag, bytes) -> {
            }));
        }
    }

}
