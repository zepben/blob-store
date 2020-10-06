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
 * Interface that represents a blob store providing access to readers and writers
 * of that store.
 */
@EverythingIsNonnullByDefault
public interface BlobStore extends AutoCloseable {

    /**
     * Gets a reader for this store
     *
     * @return reader for this store
     */
    BlobReader reader();

    /**
     * Gets a writer for this store
     *
     * @return writer for this store
     */
    BlobWriter writer();

    @Override
    void close() throws BlobStoreException;

}
