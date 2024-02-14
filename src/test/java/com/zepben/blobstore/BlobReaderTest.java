/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class BlobReaderTest {

    private BlobReader mockReader() {
        return new SampleBlobReader();
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
            return null;
        });
    }

    @Test
    public void forEachNullTag() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        mockReader().forEach(Collections.singleton("id"), "null", (id, tag, item) -> {
            called.set(true);
            return null;
        });
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
            return null;
        }));
    }

}
