/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

class SampleBlobReader : BlobReader {

    override fun ids(idHandler: (String) -> Unit) {
        idHandler("id1")
        idHandler("id2")
    }

    override fun ids(tag: String, idHandler: (String) -> Unit) {
        idHandler("id1")
        idHandler("id2")
    }

    override fun forEach(
        ids: Collection<String>,
        tags: Set<String>,
        handler: TagsHandler
    ) {
        for (id in ids) {
            val blobs = tags.associateWith { if (it == "null") null else expectedBytes(id, it) }
            handler(id, blobs)
        }
    }

    override fun forAll(
        tags: Set<String>,
        whereBlobs: List<WhereBlob>,
        handler: TagsHandler
    ) {
        val ids: List<String> = mutableListOf("id1", "id2")
        for (id in ids) {
            val blobs = tags.associateWith { if (it == "null") null else expectedBytes(id, it) }
            handler(id, blobs)
        }
    }

    override fun close() {}

    companion object {

        fun expectedBytes(id: String, tag: String): ByteArray = (id + tag).toByteArray()

    }

}
