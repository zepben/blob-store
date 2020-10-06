/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.itemwrappers;

import com.zepben.annotations.EverythingIsNonnullByDefault;
import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.BlobWriter;

import java.time.LocalDate;

@EverythingIsNonnullByDefault
public class ItemBlobWriter {

    private BlobWriter writer;
    private String id;
    private LocalDate date;
    private ByDateItemError onError;
    private boolean allGood = true;

    ItemBlobWriter(BlobWriter writer, String id, LocalDate date, ByDateItemError onError) {
        this.writer = writer;
        this.id = id;
        this.date = date;
        this.onError = onError;
    }

    public String id() {
        return id;
    }

    public LocalDate date() {
        return date;
    }

    @SuppressWarnings("WeakerAccess")
    public boolean write(String tag, byte[] bytes) {
        return write(tag, bytes, 0, bytes.length);
    }

    @SuppressWarnings("WeakerAccess")
    public boolean write(String tag, byte[] bytes, int offset, int length) {
        boolean status = true;
        try {
            // Attempt to do an update first.
            status = writer.update(id, tag, bytes, offset, length);
            // If we can't update, try a write.
            if (!status) {
                status = writer.write(id, tag, bytes, offset, length);
            }
        } catch (BlobStoreException e) {
            onError.handle(id, date, "Failed to write", e);
            status = false;
        } finally {
            allGood &= status;
        }

        return status;
    }

    @SuppressWarnings("WeakerAccess")
    public boolean delete(String tag) {
        boolean status = true;
        try {
            writer.delete(id, tag);
        } catch (BlobStoreException e) {
            onError.handle(id, date, "Failed to delete", e);
            status = false;
        } finally {
            allGood &= status;
        }
        return status;
    }

    @SuppressWarnings("WeakerAccess")
    public boolean anyFailed() {
        return !allGood;
    }

}
