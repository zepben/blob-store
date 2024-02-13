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
import java.time.LocalDate

class ItemBlobWriter(
    private val writer: BlobWriter,
    val id: String,
    val date: LocalDate,
    private val onError: ByDateItemError
) {

    private var allGood = true

    fun write(tag: String, bytes: ByteArray, offset: Int = 0, length: Int = bytes.size - offset): Boolean =
        tryOperation("write") {
            // Attempt to do an update first.
            var status = writer.update(id, tag, bytes, offset, length)

            // If we can't update, try a write.
            if (!status)
                status = writer.write(id, tag, bytes, offset, length)

            status
        }

    fun delete(tag: String): Boolean =
        tryOperation("delete") {
            writer.delete(id, tag)
        }

    fun anyFailed(): Boolean {
        return !allGood
    }

    private fun tryOperation(description: String, operationBlock: () -> Boolean): Boolean =
        try {
            operationBlock().also { allGood = allGood and it }
        } catch (e: BlobStoreException) {
            onError.handle(id, date, "Failed to $description", e)
            allGood = false
            false
        }

}
