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
import java.time.ZoneId

class ByDateItemWriter(
    private val timeZone: ZoneId,
    private val writerProvider: ByDateBlobWriterProvider
) {

    private var writeCache: MutableMap<LocalDate, BlobWriter> = HashMap()

    fun <R> write(id: String, date: LocalDate, item: R, writeHandler: (ItemBlobWriter, R) -> Unit, onError: ByDateItemError): Boolean {
        try {
            val writer = getBlobWriter(date)
            val itemBlobWriter = ItemBlobWriter(writer, id, date, onError)

            writeHandler(itemBlobWriter, item)

            if (!itemBlobWriter.anyFailed())
                return true

            onError.handle(id, date, "Failed to write", null)
        } catch (e: BlobStoreException) {
            onError.handle(id, date, "Failed to write", e)
        }

        return false
    }

    fun commit(onError: ByDateItemError): Boolean {
        val failed = mutableMapOf<LocalDate, BlobWriter>()
        writeCache.forEach { (date, writer) ->
            try {
                writer.commit()
            } catch (e: BlobStoreException) {
                failed[date] = writer
                onError.handle("", date, "Failed to commit", e)
            }
        }
        writeCache = failed
        return writeCache.isEmpty()
    }

    fun rollback(onError: ByDateItemError): Boolean {
        var status = true
        writeCache.forEach { (date: LocalDate, writer: BlobWriter) ->
            try {
                writer.rollback()
            } catch (e: BlobStoreException) {
                onError.handle("", date, "Failed to rollback", e)
                status = false
            }
        }
        writeCache.clear()
        return status
    }

    private fun getBlobWriter(date: LocalDate): BlobWriter =
        writeCache.getOrPut(date) {
            writerProvider.getWriter(date, timeZone)
        }

}
