/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

/**
 * An interface that is to be used by an [BlobStore] to provide just writing/updating functionality.
 *
 * It is up to the implementation to determine if commit is required to be called to persist data
 * or if writes and updates persist immediately. This will likely depend on if the backing data store
 * supports transactions or not.
 */
interface BlobWriter : AutoCloseable {

    //todo remove
    @Throws(BlobStoreException::class, BlobReadWriteException::class)
    fun write(id: String, tag: String, buffer: ByteArray): Boolean = write(id, tag, buffer, 0, buffer.size)
    @Throws(BlobStoreException::class, BlobReadWriteException::class)
    fun update(id: String, tag: String, buffer: ByteArray): Boolean = update(id, tag, buffer, 0, buffer.size)

    /**
     * Write a blob associating it with a given tag.
     *
     * @param id     the ID associated with the blob and tag.
     * @param tag    a tag to associate the item with.
     * @param buffer a byte array containing the blob.
     * @param offset the offset into the buffer to start reading from.
     * @param length the number of bytes to read from the buffer.
     * @return true if the [write] was successful.
     * @throws BlobStoreException if there was an error writing to the store/
     */
    @Throws(BlobStoreException::class)
    fun write(id: String, tag: String, buffer: ByteArray, offset: Int = 0, length: Int = buffer.size - offset): Boolean

    /**
     * Updates an existing blob with the associated tag
     *
     * @param id     the ID associated with the blob and tag.
     * @param tag    a tag to associate the blob with.
     * @param buffer a byte array containing the blob.
     * @param offset the offset into the blob to start reading from.
     * @param length the number of bytes to read from the blob.
     * @return true if the [update] was successful.
     * @throws BlobStoreException if there was an error writing to the store/
     */
    @Throws(BlobStoreException::class)
    fun update(id: String, tag: String, buffer: ByteArray, offset: Int = 0, length: Int = buffer.size - offset): Boolean

    /**
     * Delete a blob for a given id.
     *
     * @param id the id of the item.
     * @return true if the [delete] was successful.
     * @throws BlobStoreException if there was an error deleting from the store/
     */
    @Throws(BlobStoreException::class)
    fun delete(id: String): Boolean

    /**
     * Delete a blob for a given id and tag.
     *
     * @param id  the id of the item.
     * @param tag the tag associated with this id
     * @return true if the [delete] was successful.
     * @throws BlobStoreException if there was an error deleting from the store/
     */
    @Throws(BlobStoreException::class)
    fun delete(id: String, tag: String): Boolean

    /**
     * Commits writes/updates if supported by the implementation.
     *
     * @throws BlobStoreException when a commit error occurs.
     */
    @Throws(BlobStoreException::class)
    fun commit()

    /**
     * Rollback writes/updates if supported by the implementation.
     *
     * @throws BlobStoreException when a rollback error occurs.
     */
    @Throws(BlobStoreException::class)
    fun rollback()

    @Throws(BlobStoreException::class)
    override fun close()

}
