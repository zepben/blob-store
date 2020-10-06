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
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

@EverythingIsNonnullByDefault
@SuppressWarnings("WeakerAccess")
public class ByDateItemWriter {

    private final ZoneId timeZone;
    private final ByDateBlobWriterProvider writerProvider;
    private Map<LocalDate, BlobWriter> writeCache = new HashMap<>();

    public ByDateItemWriter(ZoneId timeZone,
                            ByDateBlobWriterProvider writerProvider) {
        this.timeZone = timeZone;
        this.writerProvider = writerProvider;
    }

    public <R> boolean write(String id,
                             LocalDate date,
                             R item,
                             BiConsumer<ItemBlobWriter, R> writeHandler,
                             ByDateItemError onError) {
        try {
            BlobWriter writer = getBlobWriter(date);
            ItemBlobWriter itemBlobWriter = new ItemBlobWriter(writer, id, date, onError);
            writeHandler.accept(itemBlobWriter, item);

            if (itemBlobWriter.anyFailed()) {
                onError.handle(id, date, "Failed to write", null);
                return false;
            }

            return true;
        } catch (BlobStoreException e) {
            onError.handle(id, date, "Failed to write", e);
            return false;
        }
    }

    public boolean commit(ByDateItemError onError) {
        Map<LocalDate, BlobWriter> failed = new HashMap<>();
        writeCache.forEach((date, writer) -> {
            try {
                writer.commit();
            } catch (BlobStoreException e) {
                failed.put(date, writer);
                onError.handle("", date, "Failed to commit", e);
            }
        });
        writeCache = failed;
        return writeCache.isEmpty();
    }

    public boolean rollback(ByDateItemError onError) {
        AtomicBoolean status = new AtomicBoolean(true);
        writeCache.forEach((date, writer) -> {
            try {
                writer.rollback();
            } catch (BlobStoreException e) {
                onError.handle("", date, "Failed to rollback", e);
                status.set(false);
            }
        });
        writeCache.clear();
        return status.get();
    }

    private BlobWriter getBlobWriter(LocalDate date) throws BlobStoreException {
        BlobWriter writer = writeCache.get(date);
        if (writer == null) {
            writer = writerProvider.getWriter(date, timeZone);
            writeCache.put(date, writer);
        }
        return writer;
    }

}
