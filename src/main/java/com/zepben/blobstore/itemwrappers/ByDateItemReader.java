/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.itemwrappers;

import com.zepben.annotations.EverythingIsNonnullByDefault;
import com.zepben.blobstore.BlobReader;
import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.Captor;
import com.zepben.blobstore.WhereBlob;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

@SuppressWarnings("WeakerAccess")
@EverythingIsNonnullByDefault
public class ByDateItemReader<T> {

    private final ZoneId timeZone;
    private final ByDateBlobReaderProvider readerProvider;
    private Map<String, ByDateTagDeserialiser> tagDeserialisers = new HashMap<>();
    private ByDateItemDeserialiser<T> itemDeserialiser = (id, date, blobs) -> null;

    public ByDateItemReader(ZoneId timeZone,
                            ByDateBlobReaderProvider readerProvider) {
        this.timeZone = timeZone;
        this.readerProvider = readerProvider;
    }

    public void setDeserialisers(ByDateItemDeserialiser<T> itemDeserialiser,
                                 Map<String, ByDateTagDeserialiser> tagDeserialisers) {
        this.itemDeserialiser = itemDeserialiser;
        this.tagDeserialisers = tagDeserialisers;
    }

    @Nullable
    public T get(String id,
                 LocalDate date,
                 ByDateItemError onError) {
        Captor<T> captor = new Captor<>();
        forEach(Collections.singleton(id), date, (i, d, item) -> captor.capture(item), onError);
        return captor.getCapture();
    }

    public void forEach(Collection<String> ids,
                        LocalDate date,
                        ByDateItemHandler<T> onRead,
                        ByDateItemError onError) {
        BlobReader blobReader;
        try {
            blobReader = getBlobReader(date);
            if (blobReader != null)
                blobReader.forEach(ids, tagDeserialisers.keySet(), tagsHandler(date, onRead, onError));
        } catch (BlobStoreException e) {
            onError.handle("", date, "Error reading data store", e);
        }
    }

    public void forAll(LocalDate date, ByDateItemHandler<T> onRead, ByDateItemError onError) {
        BlobReader blobReader;
        try {
            blobReader = getBlobReader(date);
            if (blobReader != null)
                blobReader.forAll(tagDeserialisers.keySet(), tagsHandler(date, onRead, onError));
        } catch (BlobStoreException e) {
            onError.handle("", date, "Error reading data store", e);
        }
    }

    public void forAll(LocalDate date,
                       List<WhereBlob> whereBlobs,
                       ByDateItemHandler<T> onRead,
                       ByDateItemError onError) {
        BlobReader blobReader;
        try {
            blobReader = getBlobReader(date);
            if (blobReader != null)
                blobReader.forAll(tagDeserialisers.keySet(), whereBlobs, tagsHandler(date, onRead, onError));
        } catch (BlobStoreException e) {
            onError.handle("", date, "Error reading data store", e);
        }
    }

    @Nullable
    public <R> R get(String id,
                     LocalDate date,
                     String tag,
                     ByDateItemError onError) {
        try {
            BlobReader blobReader = getBlobReader(date);
            if (blobReader != null) {
                byte[] bytes = blobReader.get(id, tag);
                if (bytes != null)
                    return deserialiseTag(id, date, tag, bytes, onError);
            }
        } catch (BlobStoreException e) {
            onError.handle("", date, "Error reading data store", e);
        }

        return null;
    }

    public <R> void forEach(Collection<String> ids,
                            LocalDate date,
                            String tag,
                            ByDateItemHandler<R> onRead,
                            ByDateItemError onError) {
        try {
            BlobReader blobReader = getBlobReader(date);
            if (blobReader != null)
                blobReader.forEach(ids, tag, blobHandler(date, onRead, onError));
        } catch (BlobStoreException e) {
            onError.handle("", date, "Error reading data store", e);
        }
    }

    public <R> void forAll(LocalDate date,
                           String tag,
                           ByDateItemHandler<R> onRead,
                           ByDateItemError onError) {
        try {
            BlobReader blobReader = getBlobReader(date);
            if (blobReader != null)
                blobReader.forAll(tag, blobHandler(date, onRead, onError));
        } catch (BlobStoreException e) {
            onError.handle("", date, "Error reading data store", e);
        }
    }

    private BlobReader.TagsHandler tagsHandler(LocalDate date,
                                               ByDateItemHandler<T> onRead,
                                               ByDateItemError onError) {
        return (id, blobs) -> {
            try {
                T item = itemDeserialiser.deserialise(id, date, blobs);
                if (item != null)
                    onRead.handle(id, date, item);
            } catch (DeserialiseException e) {
                onError.handle(id, date, e.getMessage(), e);
            }
        };
    }

    private <R> BlobReader.BlobHandler blobHandler(LocalDate date,
                                                   ByDateItemHandler<R> onRead,
                                                   ByDateItemError onError) {
        return (id, tag, blob) -> {
            R item = deserialiseTag(id, date, tag, blob, onError);
            if (item != null)
                onRead.handle(id, date, item);
        };
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private <R> R deserialiseTag(String id,
                                 LocalDate date,
                                 String tag,
                                 byte[] blob,
                                 ByDateItemError onError) {
        try {
            ByDateTagDeserialiser deserialiser = tagDeserialisers.get(tag);
            if (deserialiser == null) {
                return null;
            }

            return (R) deserialiser.deserialise(id, date, tag, blob);
        } catch (DeserialiseException e) {
            onError.handle(id, date, e.getMessage(), e);
            return null;
        }
    }

    @Nullable
    private BlobReader getBlobReader(LocalDate date) throws BlobStoreException {
        return readerProvider.getReader(date, timeZone);
    }

}
