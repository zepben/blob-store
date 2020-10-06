/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import com.zepben.annotations.EverythingIsNonnullByDefault;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class BlobReaderTest {

    @EverythingIsNonnullByDefault
    private BlobReader mockReader() {
        return new BlobReader() {
            @Override
            public void ids(Consumer<String> idHandler) {
                idHandler.accept("id1");
                idHandler.accept("id2");
            }

            @Override
            public void ids(String tag, Consumer<String> idHandler) {
                idHandler.accept("id1");
                idHandler.accept("id2");
            }

            @Override
            public void forEach(Collection<String> ids,
                                Set<String> tags,
                                TagsHandler handler) {
                for (String id : ids) {
                    Map<String, byte[]> blobs = new HashMap<>();
                    for (String tag : tags)
                        if (tag.equals("null"))
                            blobs.put(tag, null);
                        else
                            blobs.put(tag, expectedBytes(id, tag));
                    handler.handle(id, blobs);
                }
            }

            @Override
            public void forAll(Set<String> tags,
                               List<WhereBlob> whereBlobs,
                               TagsHandler handler) {
                List<String> ids = Arrays.asList("id1", "id2");
                for (String id : ids) {
                    Map<String, byte[]> blobs = new HashMap<>();
                    for (String tag : tags) {
                        if (tag.equals("null"))
                            blobs.put(tag, null);
                        else
                            blobs.put(tag, expectedBytes(id, tag));
                    }

                    handler.handle(id, blobs);
                }
            }

            @Override
            public void close() {
            }
        };
    }

    private byte[] expectedBytes(String id, String tag) {
        return (id + tag).getBytes();
    }

    @Test
    public void defaultIds() throws Exception {
        Set<String> tag = mockReader().ids();
        assertThat(tag, containsInAnyOrder("id1", "id2"));
    }

    @Test
    public void defaultIdsWithTag() throws Exception {
        Set<String> tag = mockReader().ids("tag");
        assertThat(tag, containsInAnyOrder("id1", "id2"));
    }

    @Test
    public void getSingleTagSingleId() throws Exception {
        byte[] bytes = mockReader().get("id", "tag");
        assertThat(bytes, equalTo(expectedBytes("id", "tag")));
    }

    @Test
    public void getSingleTagManyIds() throws Exception {
        Map<String, byte[]> items = mockReader().get(Arrays.asList("id1", "id2"), "tag");
        assertThat(items.size(), equalTo(2));
        assertThat(items.get("id1"), equalTo(expectedBytes("id1", "tag")));
        assertThat(items.get("id2"), equalTo(expectedBytes("id2", "tag")));
    }

    @Test
    public void getSingleTagEmptyIds() throws Exception {
        Map<String, byte[]> items = mockReader().get(Collections.emptyList(), "tag");
        assertThat(items, is(emptyMap()));
    }

    @Test
    public void forEachSingleTagManyIds() throws Exception {
        mockReader().forEach(Arrays.asList("id1", "id2"), "tag", (id, tag, item) -> {
            assertThat(tag, equalTo("tag"));
            assertThat(id, anyOf(equalTo("id1"), equalTo("id2")));
            assertThat(item, equalTo(expectedBytes(id, tag)));
        });
    }

    @Test
    public void forEachNullTag() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        mockReader().forEach(Collections.singleton("id"), "null", (id, tag, item) -> called.set(true));
        assertThat(called.get(), is(false));
    }

    @Test
    public void getAllSingleTag() throws Exception {
        Map<String, byte[]> items = mockReader().getAll("tag");
        assertThat(items.size(), equalTo(2));
        assertThat(items.get("id1"), equalTo(expectedBytes("id1", "tag")));
        assertThat(items.get("id2"), equalTo(expectedBytes("id2", "tag")));
    }

    @Test
    public void forAllSingleTag() throws Exception {
        mockReader().forAll("tag", ((id, tag, item) -> {
            assertThat(tag, equalTo("tag"));
            assertThat(id, anyOf(equalTo("id1"), equalTo("id2")));
            assertThat(item, equalTo(expectedBytes(id, tag)));
        }));
    }

}
