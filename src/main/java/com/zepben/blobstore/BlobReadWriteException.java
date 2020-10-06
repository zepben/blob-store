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

/**
 * Exception that can be thrown when an issue has occured with a specific item
 */
@EverythingIsNonnullByDefault
public class BlobReadWriteException extends BlobStoreException {

    private final String itemId;

    public BlobReadWriteException(String itemId, String message, @Nullable Throwable cause) {
        super(message, cause);
        this.itemId = itemId;
    }

    public String itemId() {
        return itemId;
    }

}
