/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.Test

class BlobReaderTest {

    private val reader = SampleBlobReader()

    @Test
    fun defaultIds() {
        assertThat(reader.ids(), containsInAnyOrder("id1", "id2"))
    }

    @Test
    fun defaultIdsWithTag() {
        assertThat(reader.ids("tag"), containsInAnyOrder("id1", "id2"))
    }

    @Test
    fun getSingleTagSingleId() {
        assertThat(reader["id", "tag"], equalTo(expectedBytes("id", "tag")))
    }

    @Test
    fun getSingleTagManyIds() {
        reader[mutableListOf("id1", "id2"), "tag"].also { items ->
            assertThat(items.size, equalTo(2))
            assertThat(items["id1"], equalTo(expectedBytes("id1", "tag")))
            assertThat(items["id2"], equalTo(expectedBytes("id2", "tag")))
        }
    }

    @Test
    fun getSingleTagEmptyIds() {
        assertThat(reader[emptyList(), "tag"], anEmptyMap())
    }

    @Test
    fun forEachSingleTagManyIds() {
        reader.forEach(mutableListOf("id1", "id2"), "tag") { id, tag, item ->
            assertThat(tag, equalTo("tag"))
            assertThat(id, anyOf(equalTo("id1"), equalTo("id2")))
            assertThat(item, equalTo(expectedBytes(id, tag)))
        }
    }

    @Test
    fun forEachNullTag() {
        var called = false
        reader.forEach(setOf("id"), "null") { _, _, _ ->
            called = true
        }
        assertThat(" should have been called", !called)
    }

    @Test
    fun getAllSingleTag() {
        val items = reader.getAll("tag")
        assertThat(items.size, equalTo(2))
        assertThat(items["id1"], equalTo(expectedBytes("id1", "tag")))
        assertThat(items["id2"], equalTo(expectedBytes("id2", "tag")))
    }

    @Test
    fun forAllSingleTag() {
        reader.forAll("tag") { id, tag, item ->
            assertThat(tag, equalTo("tag"))
            assertThat(id, anyOf(equalTo("id1"), equalTo("id2")))
            assertThat(item, equalTo(expectedBytes(id, tag)))
        }
    }

    private fun expectedBytes(id: String, tag: String): ByteArray = (id + tag).toByteArray()

}
