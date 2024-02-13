/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore

/**
 * Callback interface that allows getting blobs without having to populate and return collections.
 * Useful for large queries to reduce memory footprint.
 *
 * @param id   the id of the item.
 * @param tag  The tag associated with the item.
 * @param blob the byte array containing the blob.
 */
@Suppress("KDocUnresolvedReference")
typealias BlobHandler = (id: String, tag: String, blob: ByteArray) -> Unit

typealias TagsHandler = (id: String, blobs: Map<String, ByteArray>) -> Unit

/**
 * An interface that is to be used by an [BlobStore] to provide just reading/getting functionality.
 */
interface BlobReader : AutoCloseable {

    /**
     * Gets the set of IDs that are in the store with any tag
     *
     * @return all the IDs in the store.
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Throws(BlobStoreException::class)
    fun ids(): Set<String> =
        mutableSetOf<String>().also {
            // Consume the ids into the set.
            ids(it::add)
        }

    /**
     * Gets the set of IDs that are in the store with the given tag.
     *
     * @param tag the tag to fetch IDs from.
     * @return the IDs that have this tag.
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Throws(BlobStoreException::class)
    fun ids(tag: String): Set<String> =
        mutableSetOf<String>().also {
            // Consume the ids into the set.
            ids(tag, it::add)
        }

    /**
     * Finds all ids in the store, calling the provided handler for every id.
     *
     * @param idHandler callback for each id found
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Throws(BlobStoreException::class)
    fun ids(idHandler: (String) -> Unit)

    /**
     * Finds all ids that have the given tag, calling the provided handler for every id.
     *
     * @param tag       the tag to fetch ids from.
     * @param idHandler callback for each id found
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Throws(BlobStoreException::class)
    fun ids(tag: String, idHandler: (String) -> Unit)

    /**
     * Gets a blob for the given id associated with the given tag.
     *
     * @param id  the id of the item to get.
     * @param tag the tag associated with this id.
     * @return The blob if it exists, otherwise null.
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Throws(BlobStoreException::class)
    operator fun get(id: String, tag: String): ByteArray? =
        Captor<ByteArray>().let { captor ->
            forEach(setOf(id), tag) { _, _, item -> captor.capture(item) }
            captor.captured
        }

    /**
     * Gets a collection of blobs for the given ids associated with the given tag.
     *
     * @param ids the collection of ids to get.
     * @param tag the tag associated with this id.
     * @return a map of id to blob
     * @throws BlobStoreException when there is an error getting a blob
     */
    @Throws(BlobStoreException::class)
    operator fun get(ids: Collection<String>, tag: String): Map<String, ByteArray> {
        if (ids.isEmpty())
            return emptyMap()

        return mutableMapOf<String, ByteArray>().also { items ->
            forEach(ids, tag) { id, _, item -> items[id] = item }
        }
    }

    /**
     * Reads a collection of blobs for the given ids associated with the given tag.
     * As each blob is read, it is passed to the provided handler.
     *
     * @param ids     the collection of ids to get.
     * @param tag     the tag associated with this id.
     * @param handler callback that handles blobs as they are read
     * @throws BlobStoreException when there is an error getting an item
     */
    @Throws(BlobStoreException::class)
    fun forEach(ids: Collection<String>, tag: String, handler: BlobHandler) {
        forEach(ids, setOf(tag)) { id, blobs ->
            if (blobs.isNotEmpty())
                handler(id, tag, blobs.values.iterator().next())
        }
    }

    /**
     * Allows reading multiple blob tags calling the tags handler with all the blobs for each id.
     *
     * @param ids     ids to read
     * @param tags    set of tags to get for each id
     * @param handler handler to be called with the blobs of the requested tags
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Throws(BlobStoreException::class)
    fun forEach(ids: Collection<String>, tags: Set<String>, handler: TagsHandler)

    /**
     * Gets all blobs associated with the given tag.
     *
     * @param tag the tag to get all associated blobs.
     * @return A map of id to blob
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Throws(BlobStoreException::class)
    fun getAll(tag: String): Map<String, ByteArray> =
        mutableMapOf<String, ByteArray>().also { items ->
            forAll(tag) { id, _, item -> items[id] = item }
        }

    /**
     * Reads all blobs associated with the given tag and calls the provided handler
     *
     * @param tag     the tag associated with this id.
     * @param handler callback that handles blobs as they are read
     * @throws BlobStoreException when there is an error getting an item
     */
    @Throws(BlobStoreException::class)
    fun forAll(tag: String, handler: BlobHandler) {
        forAll(setOf(tag)) { id, blobs ->
            if (blobs.isNotEmpty()) {
                handler(id, tag, blobs.values.iterator().next())
            }
        }
    }

    /**
     * Allows reading multiple tags calling the handler providing the blobs for all ids in the store.
     *
     * @param tags    set of tags to get
     * @param handler handler to be called with blobs of all available requested tags
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Throws(BlobStoreException::class)
    fun forAll(tags: Set<String>, handler: TagsHandler) {
        forAll(tags, emptyList(), handler)
    }

    /**
     * Allows reading multiple tags calling the handler providing the blobs for all ids in the store.
     *
     * @param tags       set of tags to get
     * @param whereBlobs a list of
     * @param handler    handler to be called with blobs of all available requested tags
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Throws(BlobStoreException::class)
    fun forAll(tags: Set<String>, whereBlobs: List<WhereBlob>, handler: TagsHandler)

    @Throws(BlobStoreException::class)
    override fun close()

}
