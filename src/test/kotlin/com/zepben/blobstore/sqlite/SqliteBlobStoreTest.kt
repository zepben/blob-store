/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import com.zepben.blobstore.BlobReadWriteException
import com.zepben.blobstore.BlobStoreException
import com.zepben.blobstore.WhereBlob
import com.zepben.testutils.exception.ExpectException.Companion.expect
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.sql.SQLException

class SqliteBlobStoreTest {

    private var dbFile = Files.createTempFile(Path.of(System.getProperty("java.io.tmpdir")), "store.db", "")

    @AfterEach
    fun afterEach() {
        Files.deleteIfExists(dbFile)
    }

    @Test
    fun rejectsInvalidTags() {
        expect { SqliteBlobStore(dbFile, setOf("alpha_numerics_123", "not;allowed")) }
            .toThrow<IllegalArgumentException>()
    }

    @Test
    fun storeUpdateDelete() {
        val tags = setOf("tag1", "tag2")
        SqliteBlobStore(dbFile, tags).use { store ->
            val item1 = "a"
            val item2 = "b"
            val item3 = "c"
            val bytes1 = byteArrayOf(1)
            val bytes2 = byteArrayOf(2)
            val bytes3 = byteArrayOf(3, 4, 5, 6)
            val bytes3Expected = byteArrayOf(4, 5)

            assertThat("should have written", store.writer.write(item1, "tag1", bytes1))
            assertThat("should have written", store.writer.write(item2, "tag1", bytes2))
            assertThat("should have written", store.writer.write(item3, "tag1", bytes3, 1, 2))
            store.writer.commit()

            store.reader.getAll("tag1").also { items ->
                assertThat(items[item1], equalTo(bytes1))
                assertThat(items[item2], equalTo(bytes2))
                assertThat(items[item3], equalTo(bytes3Expected))
            }

            store.reader[listOf(item2, item3), "tag1"].also { items ->
                assertThat(items[item2], equalTo(bytes2))
                assertThat(items[item3], equalTo(bytes3Expected))
            }

            val b1Update = byteArrayOf(1, 1, 1, 1)
            val b1UpdateExpected = byteArrayOf(1, 1, 1)
            assertThat("should have updated", store.writer.update(item1, "tag1", b1Update, 1, 3))
            store.writer.commit()

            val bytes = store.reader[item1, "tag1"]
            assertThat(bytes, notNullValue())
            assertThat(bytes, equalTo(b1UpdateExpected))

            store.writer.write(item1, "tag2", bytes3)
            store.writer.commit()

            assertThat("should have deleted", store.writer.delete(item1))
            store.writer.commit()

            assertThat("item1 should have been removed", !store.reader.getAll("tag1").containsKey(item1))
            assertThat("item1 should have been removed", !store.reader.getAll("tag2").containsKey(item1))

            store.writer.write(item2, "tag2", bytes3)
            store.writer.commit()

            assertThat("should have deleted", store.writer.delete(item2, "tag1"))
            store.writer.commit()

            var numCalls = 0
            store.reader.forEach(setOf(item2), tags) { _, blobs ->
                ++numCalls
                assertThat(blobs.keys, containsInAnyOrder("tag1", "tag2"))
                assertThat(blobs["tag1"], equalTo(null))
                assertThat(blobs["tag2"], equalTo(bytes3))
            }

            assertThat(numCalls, equalTo(1))
        }
    }

    @Test
    fun getMultiTags() {
        val tags = setOf("tag1", "tag2")
        SqliteBlobStore(dbFile, tags).use { store ->
            val a1 = byteArrayOf(1)
            val a2 = byteArrayOf(1, 1)
            val b = byteArrayOf(1)
            val c = byteArrayOf(1)

            store.writer.write("a", "tag1", a1)
            store.writer.write("b", "tag1", b)
            store.writer.write("a", "tag2", a2)
            store.writer.write("c", "tag2", c)
            store.writer.commit()

            var numCalls = 0
            store.reader.forEach(listOf("a", "b", "c"), tags) { id, blobs ->
                ++numCalls
                assertThat(blobs.keys, containsInAnyOrder("tag1", "tag2"))
                when (id) {
                    "a" -> {
                        assertThat(blobs["tag1"], equalTo(a1))
                        assertThat(blobs["tag2"], equalTo(a2))
                    }

                    "b" -> {
                        assertThat(blobs["tag1"], equalTo(b))
                        assertThat(blobs["tag2"], equalTo(null))
                    }

                    "c" -> {
                        assertThat(blobs["tag1"], equalTo(null))
                        assertThat(blobs["tag2"], equalTo(c))
                    }

                    else -> fail("unexpected id: $id")
                }
            }

            assertThat(numCalls, equalTo(3))
        }
    }

    @Test
    fun missingTagsThrows() {
        expect {
            SqliteBlobStore(dbFile, setOf("tag")).use { store ->
                store.reader.forAll(setOf("missing")) { _, _ -> }
            }
        }.toThrow<BlobStoreException>()
    }

    @Test
    fun forAllWhere() {
        val tags = setOf("tag1")
        SqliteBlobStore(dbFile, tags).use { store ->
            val b1 = byteArrayOf(1, 2)
            val b2 = byteArrayOf(3, 4)

            store.writer.write("id1", "tag1", b1)
            store.writer.write("id2", "tag1", b2)

            val matched = mutableListOf<String>()
            store.reader.forAll(tags, listOf(WhereBlob.equals("tag1", b1))) { id, _ -> matched.add(id) }
            assertThat(matched, contains("id1"))

            matched.clear()
            store.reader.forAll(tags, listOf(WhereBlob.notEqual("tag1", b1))) { id, _ -> matched.add(id) }
            assertThat(matched, contains("id2"))
        }
    }

    @Test
    fun rollsBack() {
        val tag = "tests"
        SqliteBlobStore(dbFile, setOf(tag)).use { store ->
            val b1 = byteArrayOf(1)
            store.writer.write("a", tag, b1)
            store.writer.commit()

            var readBytes = store.reader["a", tag]
            assertThat(readBytes, equalTo(b1))

            val b1Update = byteArrayOf(1, 1)
            store.writer.update("a", tag, b1Update)
            store.writer.rollback()

            readBytes = store.reader["a", tag]
            assertThat(readBytes, equalTo(b1))
        }
    }

    @Test
    fun metadata() {
        SqliteBlobStore(dbFile, setOf()).use { store ->
            assertThat("should have written", store.writer.writeMetadata("test", "value1"))
            assertThat(store.reader.getMetadata("test"), equalTo("value1"))

            assertThat("should have updated", store.writer.updateMetadata("test", "value2"))
            assertThat(store.reader.getMetadata("test"), equalTo("value2"))

            assertThat("should have deleted", store.writer.deleteMetadata("test"))
            assertThat(store.reader.getMetadata("test"), equalTo(null))
        }
    }

    @Test
    fun writeThrowsOnIdViolation() {
        val tag = "tests"
        expect {
            SqliteBlobStore(dbFile, setOf(tag)).use { store ->
                try {
                    store.writer.write("a", tag, byteArrayOf(1))
                    store.writer.write("a", tag, byteArrayOf(1))
                } catch (ex: BlobReadWriteException) {
                    assertThat(ex.itemId, equalTo("a"))
                    assertThat(ex.cause, instanceOf(SQLException::class.java))
                    throw ex
                }
            }
        }.toThrow<BlobReadWriteException>()
    }

    // The SQLite driver has a bug where if an exception is thrown all existing prepared statements become invalid.
    // This test should test all prepared statements again after an exception is thrown
    @Test
    fun resetsPreparedStatementsAfterException() {
        val tag = "tests"
        SqliteBlobStore(dbFile, setOf(tag)).use { store ->
            try {
                // Force db to throw an exception
                store.writer.write("a", tag, byteArrayOf(1))
                store.writer.write("a", tag, byteArrayOf(1))
            } catch (ex: Exception) {
                // Use the prepared statements again.
                assertThat("should have written", store.writer.write("b", tag, byteArrayOf(1)))
                assertThat("should have updated", store.writer.update("b", tag, byteArrayOf(1)))
                assertThat("should have deleted", store.writer.delete("b"))
                store.writer.commit()
                store.writer.rollback()
            }
        }
    }

    @Test
    fun deleteRejectsIncorrectId() {
        val tag = "tests"
        SqliteBlobStore(dbFile, setOf(tag)).use { store ->
            assertThat("should not have deleted", !store.writer.delete("a", tag))
        }
    }

    @Test
    fun deleteRejectsOnMultipleDeleteForSameId() {
        val tag = "tests"
        SqliteBlobStore(dbFile, setOf(tag)).use { store ->
            store.writer.write("a", tag, byteArrayOf(1))

            assertThat("should have deleted the first time", store.writer.delete("a", tag))
            assertThat("shouldn't have deleted the second time", !store.writer.delete("a", tag))
        }
    }

    @Test
    fun getsIds() {
        SqliteBlobStore(dbFile, setOf("tag1", "tag2")).use { store ->
            store.writer.write("id1", "tag1", byteArrayOf(1))
            store.writer.write("id2", "tag2", byteArrayOf(1))
            store.writer.write("idboth", "tag1", byteArrayOf(1))
            store.writer.write("idboth", "tag2", byteArrayOf(1))

            assertThat(store.reader.ids(), containsInAnyOrder("id1", "id2", "idboth"))
        }
    }

    @Test
    fun getsIdsWithTag() {
        SqliteBlobStore(dbFile, setOf("tag1", "tag2")).use { store ->
            store.writer.write("id1", "tag1", byteArrayOf(1))
            store.writer.write("id2", "tag2", byteArrayOf(1))
            store.writer.write("idboth", "tag1", byteArrayOf(1))
            store.writer.write("idboth", "tag2", byteArrayOf(1))

            assertThat(store.reader.ids("tag1"), containsInAnyOrder("id1", "idboth"))
            assertThat(store.reader.ids("tag2"), containsInAnyOrder("id2", "idboth"))
        }
    }

    //TODO: Test that gets more than 1000 IDs to validate split queries works

    @Test
    fun forEachWithEmptyIdsDoesNothing() {
        val tags = setOf("tag1", "tag2")
        SqliteBlobStore(dbFile, tags).use { store ->
            var counter = 0
            store.reader.forEach(emptyList(), tags) { _, _ ->
                ++counter
            }
            assertThat(counter, equalTo(0))
        }
    }

    @Test
    fun forEachWithEmptyTagsDoesNothing() {
        SqliteBlobStore(dbFile, setOf("tag1", "tag2")).use { store ->
            var counter = 0
            store.reader.forEach(listOf("id"), emptySet()) { _, _ ->
                ++counter
            }
            assertThat(counter, equalTo(0))
        }
    }

    @Test
    fun writeFailsWithUnknownTag() {
        val tag = "tests"
        expect {
            SqliteBlobStore(dbFile, setOf(tag)).use { store ->
                store.writer.write("a", "unknown", byteArrayOf(1))
            }
        }.toThrow<IllegalArgumentException>()
    }

    @Test
    fun updateFailsWithUnknownTag() {
        val tag = "tests"
        expect {
            SqliteBlobStore(dbFile, setOf(tag)).use { store ->
                store.writer.update("a", "unknown", byteArrayOf(1))
            }
        }.toThrow<IllegalArgumentException>()
    }

    @Test
    fun deleteFailsWithUnknownTag() {
        val tag = "tests"
        expect {
            SqliteBlobStore(dbFile, setOf(tag)).use { store ->
                store.writer.delete("a", "unknown")
            }
        }.toThrow<IllegalArgumentException>()
    }

    @Test
    fun forEachFailsWithUnknownKey() {
        expect {
            SqliteBlobStore(dbFile, setOf("tag")).use { store ->
                store.reader.forEach(setOf("a"), "unknown") { _, _, _ -> }
            }
        }.toThrow<BlobStoreException>()
    }

    @Test
    fun forAllManyFailsWithUnknownKey() {
        expect {
            SqliteBlobStore(dbFile, setOf("tag")).use { store ->
                store.reader.forAll("unknown") { _, _, _ -> }
            }
        }.toThrow<BlobStoreException>()
    }

    @Test
    fun handlesLargestSqliteParamCountQuery() {
        val ids = (0..SqliteBlobReader.MAX_PARAM_IDS_IN_QUERY).map { it.toString() }
        SqliteBlobStore(dbFile, setOf("tag")).use { store ->
            store.reader.forEach(ids, "tag") { _, _, _ -> }
        }
    }

}
