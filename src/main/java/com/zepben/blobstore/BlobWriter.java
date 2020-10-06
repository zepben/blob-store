/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import com.zepben.annotations.EverythingIsNonnullByDefault;

/**
 * An interface that is to be used by an {@link BlobStore} to provide just writing/updating functionality.
 * <p>It is up to the implementation to determine if commit is required to be called to persist data
 * or if writes and updates persist immediately. This will likely depend on if the backing data store
 * supports transactions or not.
 */
@EverythingIsNonnullByDefault
public interface BlobWriter extends AutoCloseable {

    /**
     * Write a blob associating it with the given tag.
     *
     * @param id   the ID associated with the blob and tag
     * @param tag  a tag to associate the item with.
     * @param blob the bytes to write.
     * @return true if the write was successful.
     * @throws BlobStoreException if there was an error writing to the store/
     */
    default boolean write(String id, String tag, byte[] blob) throws BlobStoreException {
        return write(id, tag, blob, 0, blob.length);
    }

    /**
     * Write a blob associating it with a given tag.
     *
     * @param id     the ID associated with the blob and tag.
     * @param tag    a tag to associate the item with.
     * @param buffer a byte array containing the blob.
     * @param offset the offset into the buffer to start reading from.
     * @param length the number of bytes to read from the buffer.
     * @return true if the write was successful.
     * @throws BlobStoreException if there was an error writing to the store/
     */
    boolean write(String id, String tag, byte[] buffer, int offset, int length) throws BlobStoreException;

    /**
     * Update an item that is associated with the given tag.
     *
     * @param id   the ID of the item
     * @param tag  a tag to associate the item with.
     * @param blob the bytes to write.
     * @return true if the write was successful.
     * @throws BlobStoreException if there was an error writing the item.
     */
    default boolean update(String id, String tag, byte[] blob) throws BlobStoreException {
        return update(id, tag, blob, 0, blob.length);
    }

    /**
     * Updates an existing blob with the associated tag
     *
     * @param id     the ID associated with the blob and tag.
     * @param tag    a tag to associate the blob with.
     * @param buffer a byte array containing the blob.
     * @param offset the offset into the buffer to start reading from.
     * @param length the number of bytes to read from the buffer.
     * @return true if the update was successful.
     * @throws BlobStoreException if there was an error writing to the store/
     */
    boolean update(String id, String tag, byte[] buffer, int offset, int length) throws BlobStoreException;

    /**
     * Delete a blob for a given id.
     *
     * @param id the id of the item.
     * @return true if the delete was successful.
     * @throws BlobStoreException if there was an error deleting from the store/
     */
    boolean delete(String id) throws BlobStoreException;

    /**
     * Delete a blob for a given id and tag.
     *
     * @param id  the id of the item.
     * @param tag the tag associated with this id
     * @return true if the delete was successful.
     * @throws BlobStoreException if there was an error deleting from the store/
     */
    boolean delete(String id, String tag) throws BlobStoreException;

    /**
     * Commits writes/updates if supported by the implementation.
     *
     * @throws BlobStoreException when a commit error occurs.
     */
    void commit() throws BlobStoreException;

    /**
     * Rollback writes/updates if supported by the implementation.
     *
     * @throws BlobStoreException when a rollback error occurs.
     */
    void rollback() throws BlobStoreException;

    @Override
    void close() throws BlobStoreException;

}
