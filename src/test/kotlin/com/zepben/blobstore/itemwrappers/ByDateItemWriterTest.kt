/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.itemwrappers

import com.zepben.blobstore.BlobStoreException
import com.zepben.blobstore.BlobWriter
import io.mockk.*
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.ZoneId

class ByDateItemWriterTest {

    private val blobWriter = mockk<BlobWriter>()
    private val blobWriterProvider = mockk<ByDateBlobWriterProvider>()
    private val writeHandler = mockk<Function2<ItemBlobWriter, Any, Unit>>()
    private val itemError = mockk<ByDateItemError>()

    private val timeZone = ZoneId.systemDefault()
    private var itemWriter: ByDateItemWriter = ByDateItemWriter(timeZone, blobWriterProvider)

    @Test
    fun write() {
        val id = "id"
        val date = LocalDate.now()
        val item = Any()

        justRun { writeHandler.invoke(any(), any()) }
        every { blobWriter.write(any(), any(), any(), any(), any()) } returns true
        every { blobWriterProvider.getWriter(any(), any()) } returns blobWriter

        assertThat("should have written", itemWriter.write("id", LocalDate.now(), item, writeHandler, itemError))

        verify { blobWriterProvider.getWriter(LocalDate.now(), timeZone) }

        val itemBlobWriterSlot = slot<ItemBlobWriter>()
        val itemSlot = slot<Any>()
        verify { writeHandler.invoke(capture(itemBlobWriterSlot), capture(itemSlot)) }

        assertThat(itemSlot.captured, equalTo(item))
        assertThat(itemBlobWriterSlot.captured.id, equalTo(id))
        assertThat(itemBlobWriterSlot.captured.date, equalTo(date))
    }

    @Test
    fun writeHandlesBlobstoreException() {
        val ex = BlobStoreException("test", null)
        val item = Any()

        every { blobWriterProvider.getWriter(any(), any()) } throws ex
        justRun { itemError.handle(any(), any(), any(), any()) }

        itemWriter.write("errorId", LocalDate.now(), item, writeHandler, itemError)

        verify { itemError.handle(eq("errorId"), eq(LocalDate.now()), any(), eq(ex)) }
        verify { writeHandler wasNot Called }
    }

    @Test
    fun commits() {
        val item = Any()
        val blobWriter1 = mockk<BlobWriter> { justRun { commit() } }
        val blobWriter2 = mockk<BlobWriter> { justRun { commit() } }

        justRun { writeHandler.invoke(any(), any()) }
        every { blobWriterProvider.getWriter(any(), any()) } returns blobWriter1 andThen blobWriter2

        itemWriter.write("id", LocalDate.now().minusDays(1), item, writeHandler, itemError)
        itemWriter.write("id", LocalDate.now(), item, writeHandler, itemError)

        assertThat("should have committed", itemWriter.commit(itemError))

        verifySequence {
            blobWriter1.commit()
            blobWriter2.commit()
        }

    }

    @Test
    fun rollsBack() {
        val item = Any()
        val blobWriter1 = mockk<BlobWriter> { justRun { rollback() } }
        val blobWriter2 = mockk<BlobWriter> { justRun { rollback() } }

        justRun { writeHandler.invoke(any(), any()) }
        every { blobWriterProvider.getWriter(any(), any()) } returns blobWriter1 andThen blobWriter2

        itemWriter.write("id", LocalDate.now().minusDays(1), item, writeHandler, itemError)
        itemWriter.write("id", LocalDate.now(), item, writeHandler, itemError)

        assertThat("should have rolled back", itemWriter.rollback(itemError))

        verifySequence {
            blobWriter1.rollback()
            blobWriter2.rollback()
        }
    }

}
