/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.itemwrappers;

import com.zepben.blobstore.BlobReader;
import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.WhereBlob;

import java.util.*;
import java.util.function.Consumer;

class MockBlobReader implements BlobReader {

    private final Set<String> ids;
    private final Set<String> tags;

    MockBlobReader(Set<String> ids, Set<String> tags) {
        this.ids = ids;
        this.tags = tags;
    }

    byte[] getBlob(String tag) {
        return tag.getBytes();
    }

    Map<String, byte[]> getBlobs(Set<String> tags) {
        Map<String, byte[]> bytes = new HashMap<>();
        for (String tag : tags) {
            if (this.tags.contains(tag))
                bytes.put(tag, getBlob(tag));
        }
        return bytes;
    }

    @Override
    public void ids(Consumer<String> idHandler) {
        for (String id : ids) {
            idHandler.accept(id);
        }
    }

    @Override
    public void ids(String tag, Consumer<String> idHandler) {
        for (String id : ids) {
            idHandler.accept(id);
        }
    }

    @Override
    @SuppressWarnings("RedundantThrows")
    public void forEach(Collection<String> ids, Set<String> tags, TagsHandler handler) throws BlobStoreException {
        for (String id : ids) {
            if (ids.contains(id)) {
                Map<String, byte[]> bytes = getBlobs(tags);
                handler.handle(id, bytes);
            }
        }
    }

    @Override
    public void forAll(Set<String> tags,
                       List<WhereBlob> whereBlobs,
                       TagsHandler handler) {
        for (String id : ids) {
            Map<String, byte[]> bytes = getBlobs(tags);
            handler.handle(id, bytes);
        }
    }

    @Override
    public void close() {

    }

}
