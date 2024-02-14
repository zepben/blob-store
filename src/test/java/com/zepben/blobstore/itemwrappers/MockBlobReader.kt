/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.itemwrappers

import com.zepben.blobstore.BlobReader
import com.zepben.blobstore.BlobStoreException
import com.zepben.blobstore.WhereBlob

class MockBlobReader(private val ids: Set<String>, private val tags: Set<String>) : BlobReader {

    fun getBlob(tag: String): ByteArray {
        return tag.toByteArray()
    }

    fun getBlobs(tags: Set<String>): Map<String, ByteArray?> {
        val bytes = mutableMapOf<String, ByteArray?>()
        for (tag in tags) {
            if (this.tags.contains(tag)) bytes[tag] = getBlob(tag)
        }
        return bytes
    }

    @Throws(BlobStoreException::class)
    override fun ids(idHandler: Function1<String, Unit>) {
        for (id in ids) {
            idHandler.invoke(id)
        }
    }

    @Throws(BlobStoreException::class)
    override fun ids(tag: String, idHandler: Function1<String, Unit>) {
        for (id in ids) {
            idHandler.invoke(id)
        }
    }

    @Throws(BlobStoreException::class)
    override fun forEach(ids: Collection<String>, tags: Set<String>, handler: Function2<String, Map<String, ByteArray?>, Unit>) {
        for (id in ids) {
            if (ids.contains(id)) {
                val bytes = getBlobs(tags)
                handler.invoke(id, bytes)
            }
        }
    }

    @Throws(BlobStoreException::class)
    override fun forAll(tags: Set<String>, whereBlobs: List<WhereBlob>, handler: Function2<String, Map<String, ByteArray?>, Unit>) {
        for (id in ids) {
            val bytes = getBlobs(tags)
            handler.invoke(id, bytes)
        }
    }

    override fun close() {}

}
