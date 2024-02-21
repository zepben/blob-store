/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

/**
 * Interface that represents a blob store providing access to readers and writers
 * of that store.
 */
interface BlobStore : AutoCloseable {

    /**
     * Gets a reader for this store
     *
     * @return reader for this store
     */
    val reader: BlobReader

    /**
     * Gets a writer for this store
     *
     * @return writer for this store
     */
    val writer: BlobWriter

    @Throws(BlobStoreException::class)
    override fun close()

}
