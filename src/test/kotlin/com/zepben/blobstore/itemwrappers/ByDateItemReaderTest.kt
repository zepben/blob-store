/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.itemwrappers

import com.zepben.blobstore.BlobStoreException
import com.zepben.blobstore.CustomMatchers.eqArrayValueMap
import com.zepben.blobstore.WhereBlob
import io.mockk.*
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.nullValue
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.ZoneId

class ByDateItemReaderTest {

    private val ids: Set<String> = HashSet(mutableListOf("id1", "id2"))
    private val tags: Set<String> = HashSet(mutableListOf("tag1", "tag2"))

    private val blobReader = spyk(MockBlobReader(ids, tags))

    private val blobReaderProvider = mockk<ByDateBlobReaderProvider>() { every { getReader(any(), any()) } returns blobReader }

    private val tag1Deserialiser = mockk<ByDateTagDeserialiser<*>>()
    private val tag2Deserialiser = mockk<ByDateTagDeserialiser<*>>()
    private val tagDeserialisers = mapOf("tag1" to tag1Deserialiser, "tag2" to tag2Deserialiser)

    private val itemDeserialiser = mockk<ByDateItemDeserialiser<String>>()
    private val itemError = mockk<ByDateItemError>()
    private val itemHandler = mockk<ByDateItemHandler<String>> { justRun { handle(any(), any(), any()) } }

    private val timeZone = ZoneId.systemDefault()
    private val date = LocalDate.now()

    private val itemReader = spyk(ByDateItemReader<String>(timeZone, blobReaderProvider).apply {
        setDeserialisers(itemDeserialiser, tagDeserialisers)
    })

    @Test
    fun get() {
        val id = "id1"

        every { itemDeserialiser.deserialise(any(), any(), any()) } returns "theItem"

        val result = itemReader[id, date, itemError]

        assertThat(result, equalTo("theItem"))
        verify { itemReader.forEach(setOf(id), date, any(), itemError) }
    }

    @Test
    fun getNullBlobReaderProvider() {
        val id = "id1"

        every { blobReaderProvider.getReader(any(), any()) } returns null

        val result = itemReader[id, date, itemError]

        assertThat(result, nullValue())
    }

    @Test
    fun forEach() {
        val ids = listOf("id1")
        val expectedBytes = blobReader.getBlobs(tags).also { clearMocks(blobReader, answers = false) }

        every { itemDeserialiser.deserialise(any(), any(), any()) } returns "theItem"

        itemReader.forEach(ids, date, itemHandler, itemError)

        verify { blobReader.forEach(ids, tags, any()) }
        verify { blobReaderProvider.getReader(date, timeZone) }
        verify { itemDeserialiser.deserialise("id1", date, eqArrayValueMap(expectedBytes)) }
        verify { itemHandler.handle("id1", date, "theItem") }
    }

    @Test
    fun forEachNullBlobReaderProvider() {
        val ids = listOf("id1")

        every { blobReaderProvider.getReader(any(), any()) } returns null

        itemReader.forEach(ids, date, itemHandler, itemError)

        verify { blobReaderProvider.getReader(date, timeZone) }
    }

    @Test
    fun forAll() {
        val expectedBytes = blobReader.getBlobs(tags).also { clearMocks(blobReader, answers = false) }

        every { itemDeserialiser.deserialise("id1", any(), any()) } returns "theItem1"
        every { itemDeserialiser.deserialise("id2", any(), any()) } returns "theItem2"

        itemReader.forAll(date, itemHandler, itemError)

        verify { blobReader.forAll(tags, any()) }
        verify { blobReaderProvider.getReader(date, timeZone) }
        verify { itemDeserialiser.deserialise("id1", date, eqArrayValueMap(expectedBytes)) }
        verify { itemDeserialiser.deserialise("id2", date, eqArrayValueMap(expectedBytes)) }
        verify { itemHandler.handle("id1", date, "theItem1") }
        verify { itemHandler.handle("id2", date, "theItem2") }
    }

    @Test
    fun forAllWhere() {
        val whereBlobs = listOf(WhereBlob.equals("tag1", byteArrayOf()))
        val expectedBytes = blobReader.getBlobs(tags).also { clearMocks(blobReader, answers = false) }

        every { itemDeserialiser.deserialise(any(), any(), any()) } returns "theItem"

        itemReader.forAll(date, whereBlobs, itemHandler, itemError)

        verify { blobReader.forAll(tags, whereBlobs, any()) }
        verify { blobReaderProvider.getReader(date, timeZone) }
        verify { itemDeserialiser.deserialise("id1", date, eqArrayValueMap(expectedBytes)) }
        verify { itemHandler.handle("id1", date, "theItem") }
    }

    @Test
    fun forAllNullBlobReaderProvider() {

        every { blobReaderProvider.getReader(any(), any()) } returns null

        itemReader.forAll(date, itemHandler, itemError)

        verify { blobReaderProvider.getReader(date, timeZone) }
    }

    @Test
    fun handlesBlobReaderException() {
        val ids = setOf("id")
        val ex = BlobStoreException("test", null)

        every { blobReader.forEach(any(), any<Set<String>>(), any()) } throws ex
        every { blobReader.forAll(any<Set<String>>(), any()) } throws ex
        justRun { itemError.handle(any(), any(), any(), any()) }

        itemReader.forEach(ids, date, itemHandler, itemError)
        itemReader.forAll(date, itemHandler, itemError)

        verify(exactly = 2) { itemError.handle("", date, any(), ex) }
    }

    @Test
    fun getTag() {
        val id = "id1"
        val tag = "tag1"
        val expectedBytes = blobReader.getBlob(tag).also { clearMocks(blobReader, answers = false) }

        every { tag1Deserialiser.deserialise(any(), any(), any(), any()) } returns "theTag"

        val result = itemReader.get<String>(id, date, tag, itemError)

        assertThat(result, equalTo("theTag"))

        verify { blobReader[id, tag] }
        verify { tag1Deserialiser.deserialise(id, date, tag, expectedBytes) }
    }

    @Test
    fun getTagNullBlobReaderProvider() {
        val id = "id1"
        val tag = "tag1"

        every { blobReaderProvider.getReader(any(), any()) } returns null

        val result = itemReader.get<String>(id, date, tag, itemError)

        assertThat(result, nullValue())
    }

    @Test
    fun forEachTag() {
        val ids = listOf("id1")
        val tag = "tag1"
        val expectedBytes = blobReader.getBlob(tag).also { clearMocks(blobReader, answers = false) }

        every { tag1Deserialiser.deserialise(any(), any(), any(), any()) } returns "theTag"

        itemReader.forEach(ids, date, tag, itemHandler, itemError)

        verify { blobReader.forEach(ids, tag, any()) }
        verify { blobReaderProvider.getReader(date, timeZone) }
        verify { tag1Deserialiser.deserialise("id1", date, tag, expectedBytes) }
        verify { itemHandler.handle("id1", date, "theTag") }
    }

    @Test
    fun forEachTagNullBlobReaderProvider() {
        val ids = listOf("id1")
        val tag = "tag1"

        every { blobReaderProvider.getReader(any(), any()) } returns null

        itemReader.forEach(ids, date, tag, itemHandler, itemError)

        verify { blobReaderProvider.getReader(date, timeZone) }
    }

    @Test
    fun forAllTag() {
        val tag = "tag1"
        val expectedBytes = blobReader.getBlob(tag).also { clearMocks(blobReader, answers = false) }

        every { tag1Deserialiser.deserialise(any(), any(), any(), any()) } returns "theTag"

        itemReader.forAll(date, tag, itemHandler, itemError)

        verify { blobReader.forAll(tag, any()) }
        verify { blobReaderProvider.getReader(date, timeZone) }
        verify { tag1Deserialiser.deserialise("id1", date, tag, expectedBytes) }
        verify { tag1Deserialiser.deserialise("id2", date, tag, expectedBytes) }
        verify { itemHandler.handle("id1", date, "theTag") }
        verify { itemHandler.handle("id2", date, "theTag") }
    }

    @Test
    fun forAllTagNullBlobReaderProvider() {
        val tag = "tag1"

        every { blobReaderProvider.getReader(any(), any()) } returns null

        itemReader.forAll(date, tag, itemHandler, itemError)

        verify { blobReaderProvider.getReader(date, timeZone) }
    }

    @Test
    fun getBlobReaderHandlesException() {
        val ex = BlobStoreException("test", null)

        every { blobReaderProvider.getReader(any(), any()) } throws ex
        justRun { itemError.handle(any(), any(), any(), any()) }

        itemReader.forEach(setOf("id"), date, itemHandler, itemError)
        itemReader.forAll(date, itemHandler, itemError)
        itemReader.forEach(setOf("id"), date, "tag1", itemHandler, itemError)
        itemReader.forAll(date, "tag1", itemHandler, itemError)

        verify(exactly = 4) { itemError.handle("", date, any(), ex) }
    }

    @Test
    fun handlesItemDeserialiseException() {
        val id = "id1"
        val ex = DeserialiseException("test", null)

        every { itemDeserialiser.deserialise(any(), any(), any()) } throws ex
        justRun { itemError.handle(any(), any(), any(), any()) }

        val str = itemReader[id, date, itemError]

        assertThat(str, nullValue())
        verify { itemError.handle(id, date, "test", ex) }
    }

    @Test
    fun handlesTagDeserialiseException() {
        val id = "id1"
        val ex = DeserialiseException("test", null)

        every { tag1Deserialiser.deserialise(any(), any(), any(), any()) } throws ex
        justRun { itemError.handle(any(), any(), any(), any()) }

        val str = itemReader.get<String>(id, date, "tag1", itemError)

        assertThat(str, nullValue())
        verify { itemError.handle(id, date, "test", ex) }
    }

}
