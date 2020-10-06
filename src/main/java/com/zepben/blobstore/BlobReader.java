/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

/**
 * An interface that is to be used by an {@link BlobStore} to provide just reading/getting functionality.
 */
@EverythingIsNonnullByDefault
public interface BlobReader extends AutoCloseable {

    /**
     * Callback interface that allows getting blobs without having to populate and return collections.
     * Useful for large queries to reduce memory footprint.
     */
    @EverythingIsNonnullByDefault
    @FunctionalInterface
    interface BlobHandler {

        /**
         * The handler method that is called when an item is read.
         *
         * @param id   the id of the item.
         * @param tag  The tag associated with the item.
         * @param blob the byte array containing the blob.
         */
        void handle(String id, String tag, byte[] blob);

    }

    @EverythingIsNonnullByDefault
    interface TagsHandler {

        void handle(String id, Map<String, byte[]> blobs);

    }

    /**
     * Gets the set of IDs that are in the store with any tag
     *
     * @return all the IDs in the store.
     * @throws BlobStoreException if there is an error reading from the store.
     */
    default Set<String> ids() throws BlobStoreException {
        Set<String> ids = new HashSet<>();
        ids(ids::add);
        return ids;
    }

    /**
     * Gets the set of IDs that are in the store with the given tag.
     *
     * @param tag the tag to fetch IDs from.
     * @return the IDs that have this tag.
     * @throws BlobStoreException if there is an error reading from the store.
     */
    default Set<String> ids(String tag) throws BlobStoreException {
        Set<String> ids = new HashSet<>();
        ids(tag, ids::add);
        return ids;
    }

    /**
     * Finds all ids in the store, calling the provided handler for every id.
     *
     * @param idHandler callback for each id found
     * @throws BlobStoreException if there is an error reading from the store.
     */
    void ids(Consumer<String> idHandler) throws BlobStoreException;

    /**
     * Finds all ids that have the given tag, calling the provided handler for every id.
     *
     * @param tag       the tag to fetch ids from.
     * @param idHandler callback for each id found
     * @throws BlobStoreException if there is an error reading from the store.
     */
    void ids(String tag, Consumer<String> idHandler) throws BlobStoreException;

    /**
     * Gets a blob for the given id associated with the given tag.
     *
     * @param id  the id of the item to get.
     * @param tag the tag associated with this id.
     * @return The blob if it exists, otherwise null.
     * @throws BlobStoreException if there is an error reading from the store.
     */
    @Nullable
    default byte[] get(String id, String tag) throws BlobStoreException {
        Captor<byte[]> captor = new Captor<>();
        forEach(Collections.singleton(id), tag, (captureId, t, item) -> captor.capture(item));
        return captor.getCapture();
    }

    /**
     * Gets a collection of blobs for the given ids associated with the given tag.
     *
     * @param ids the collection of ids to get.
     * @param tag the tag associated with this id.
     * @return a map of id to blob
     * @throws BlobStoreException when there is an error getting a blob
     */
    default Map<String, byte[]> get(Collection<String> ids, String tag) throws BlobStoreException {
        if (ids.isEmpty())
            return Collections.emptyMap();

        Map<String, byte[]> items = new HashMap<>();
        forEach(ids, tag, (id, t, item) -> items.put(id, item));
        return items;
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
    default void forEach(Collection<String> ids,
                         String tag,
                         BlobHandler handler) throws BlobStoreException {
        forEach(ids, Collections.singleton(tag), (id, blobs) -> {
            if (blobs.size() > 0) {
                byte[] blob = blobs.values().iterator().next();
                if (blob != null)
                    handler.handle(id, tag, blob);
            }
        });
    }

    /**
     * Allows reading multiple blob tags calling the tags handler with all the blobs for each id.
     *
     * @param ids     ids to read
     * @param tags    set of tags to get for each id
     * @param handler handler to be called with the blobs of the requested tags
     * @throws BlobStoreException if there is an error reading from the store.
     */
    void forEach(Collection<String> ids,
                 Set<String> tags,
                 TagsHandler handler) throws BlobStoreException;

    /**
     * Gets all blobs associated with the given tag.
     *
     * @param tag the tag to get all associated blobs.
     * @return A map of id to blob
     * @throws BlobStoreException if there is an error reading from the store.
     */
    default Map<String, byte[]> getAll(String tag) throws BlobStoreException {
        Map<String, byte[]> items = new HashMap<>();
        forAll(tag, (id, t, item) -> items.put(id, item));
        return items;
    }

    /**
     * Reads all blobs associated with the given tag and calls the provided handler
     *
     * @param tag     the tag associated with this id.
     * @param handler callback that handles blobs as they are read
     * @throws BlobStoreException when there is an error getting an item
     */
    default void forAll(String tag, BlobHandler handler) throws BlobStoreException {
        forAll(Collections.singleton(tag), ((id, blobs) -> {
            if (blobs.size() > 0) {
                byte[] blob = blobs.values().iterator().next();
                if (blob != null)
                    handler.handle(id, tag, blob);
            }
        }));
    }

    /**
     * Allows reading multiple tags calling the handler providing the blobs for all ids in the store.
     *
     * @param tags    set of tags to get
     * @param handler handler to be called with blobs of all available requested tags
     * @throws BlobStoreException if there is an error reading from the store.
     */
    default void forAll(Set<String> tags,
                        TagsHandler handler) throws BlobStoreException {
        forAll(tags, Collections.emptyList(), handler);
    }

    /**
     * Allows reading multiple tags calling the handler providing the blobs for all ids in the store.
     *
     * @param tags       set of tags to get
     * @param whereBlobs a list of
     * @param handler    handler to be called with blobs of all available requested tags
     * @throws BlobStoreException if there is an error reading from the store.
     */
    void forAll(Set<String> tags,
                List<WhereBlob> whereBlobs,
                TagsHandler handler) throws BlobStoreException;

    @Override
    void close() throws BlobStoreException;

}
