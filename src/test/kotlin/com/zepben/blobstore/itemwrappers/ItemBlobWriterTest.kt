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

class ItemBlobWriterTest {

    private val blobWriter = mockk<BlobWriter>()
    private val onError = mockk<ByDateItemError>()

    private val id = "id"
    private val date = LocalDate.now()
    private val tag = "tag"
    private val blob = byteArrayOf(1, 2, 3, 4)

    private var itemWriter = ItemBlobWriter(blobWriter, id, date, onError)

    @Test
    fun idAndDateAreCorrect() {

        assertThat(itemWriter.id, equalTo(id))
        assertThat(itemWriter.date, equalTo(date))
    }

    @Test
    fun write() {
        every { blobWriter.update(id, tag, blob, 0, blob.size) } returns false
        every { blobWriter.write(id, tag, blob, 0, blob.size) } returns true

        assertThat("should have written", itemWriter.write(tag, blob))

        assertThat("nothing should have failed", !itemWriter.anyFailed())
        verify { blobWriter.write(id, tag, blob, 0, blob.size) }
    }

    @Test
    fun update() {
        every { blobWriter.update(id, tag, blob, 0, blob.size) } returns true

        assertThat("should have written", itemWriter.write(tag, blob))

        assertThat("nothing should have failed", !itemWriter.anyFailed())
        verifySequence {
            // Verify that only update was called, not write.
            blobWriter.update(id, tag, blob, 0, blob.size)
        }
    }

    @Test
    fun delete() {
        every { blobWriter.delete(id, tag) } returns true

        assertThat("should have deleted", itemWriter.delete(tag))

        assertThat("nothing should have failed", !itemWriter.anyFailed())
        verify { blobWriter.delete(id, tag) }
    }

    @Test
    fun anyFailed() {
        every { blobWriter.update(any(), any(), any(), any(), any()) } returns false
        every { blobWriter.write(any(), any(), any(), any(), any()) } returns false

        assertThat("nothing should have failed", !itemWriter.anyFailed())

        assertThat("expected write to fail", !itemWriter.write(tag, blob))

        assertThat("should have detected failure", itemWriter.anyFailed())
    }

    @Test
    fun handlesWriteException() {
        val ex = BlobStoreException("", null)

        every { blobWriter.update(id, tag, blob, 0, blob.size) } throws ex
        justRun { onError.handle(any(), any(), any(), any()) }

        assertThat("write should have failed", !itemWriter.write(tag, blob))

        assertThat("should have detected failure", itemWriter.anyFailed())
        verify { onError.handle(id, date, "Failed to write", ex) }
    }

    @Test
    fun handlesDeleteException() {
        val ex = BlobStoreException("", null)

        every { blobWriter.delete(id, tag) } throws ex
        justRun { onError.handle(any(), any(), any(), any()) }

        assertThat("delete should have failed", !itemWriter.delete(tag))

        assertThat("should have detected failure", itemWriter.anyFailed())
        verify { onError.handle(id, date, "Failed to delete", ex) }
    }

}
