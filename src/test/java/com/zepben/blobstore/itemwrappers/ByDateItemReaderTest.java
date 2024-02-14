/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.itemwrappers;

import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.WhereBlob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import static com.zepben.blobstore.CustomMatchers.eqArrayValueMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ByDateItemReaderTest {

    private Set<String> ids = new HashSet<>(Arrays.asList("id1", "id2"));
    private static Set<String> tags = new HashSet<>(Arrays.asList("tag1", "tag2"));

    @Spy private MockBlobReader blobReader = new MockBlobReader(ids, tags);
    @Mock private ByDateBlobReaderProvider blobReaderProvider;
    @Mock private ByDateTagDeserialiser tag1Deserialiser;
    @Mock private ByDateTagDeserialiser tag2Deserialiser;
    @Mock private ByDateItemDeserialiser<String> itemDeserialiser;
    @Mock private ByDateItemError itemError;
    @Mock private ByDateItemHandler<String> itemHandler;

    private ZoneId timeZone = ZoneId.systemDefault();
    private ByDateItemReader<String> itemReader;

    @BeforeEach
    public void before() throws Exception {
        MockitoAnnotations.initMocks(this);

        doReturn(blobReader).when(blobReaderProvider).getReader(any(), any());

        Map<String, ByDateTagDeserialiser<?>> tagDeserialisers = new HashMap<>();
        tagDeserialisers.put("tag1", tag1Deserialiser);
        tagDeserialisers.put("tag2", tag2Deserialiser);
        itemReader = new ByDateItemReader<>(timeZone, blobReaderProvider);
        itemReader.setDeserialisers(itemDeserialiser, tagDeserialisers);
    }

    @Test
    public void get() throws Exception {
        String id = "id1";
        LocalDate date = LocalDate.now();

        itemReader = spy(itemReader);
        doReturn("theItem").when(itemDeserialiser).deserialise(any(), any(), any());

        String result = itemReader.get(id, date, itemError);
        verify(itemReader).forEach(eq(Collections.singleton(id)), eq(date), any(), eq(itemError));
        assertThat(result, equalTo("theItem"));
    }

    @Test
    public void getNullBlobReaderProvider() throws Exception {
        String id = "id1";
        LocalDate date = LocalDate.now();

        doReturn(null).when(blobReaderProvider).getReader(any(), any());
        itemReader = spy(itemReader);
        String result = itemReader.get(id, date, itemError);
        assertThat(result, is(nullValue()));
    }

    @Test
    public void forEach() throws Exception {
        LocalDate date = LocalDate.now();
        List<String> ids = Collections.singletonList("id1");

        doReturn("theItem").when(itemDeserialiser).deserialise(any(), any(), any());
        itemReader.forEach(ids, date, itemHandler, itemError);
        verify(blobReader).forEach(eq(ids), eq(tags), any());

        verify(blobReaderProvider).getReader(date, timeZone);
        ArgumentMatcher<Map<String, byte[]>> matchesMap = eqArrayValueMap(blobReader.getBlobs(tags));
        verify(itemDeserialiser).deserialise(eq("id1"), eq(date), argThat(matchesMap));
        verify(itemHandler).handle("id1", date, "theItem");
    }

    @Test
    public void forEachNullBlobReaderProvider() throws Exception {
        LocalDate date = LocalDate.now();
        List<String> ids = Collections.singletonList("id1");

        doReturn(null).when(blobReaderProvider).getReader(any(), any());
        itemReader.forEach(ids, date, itemHandler, itemError);
        verify(blobReaderProvider).getReader(date, timeZone);
    }

    @Test
    public void forAll() throws Exception {
        LocalDate date = LocalDate.now();

        doReturn("theItem1").when(itemDeserialiser).deserialise(eq("id1"), any(), any());
        doReturn("theItem2").when(itemDeserialiser).deserialise(eq("id2"), any(), any());
        itemReader.forAll(date, itemHandler, itemError);
        verify(blobReader).forAll(eq(tags), any());

        verify(blobReaderProvider).getReader(date, timeZone);
        ArgumentMatcher<Map<String, byte[]>> matchesMap = eqArrayValueMap(blobReader.getBlobs(tags));
        verify(itemDeserialiser).deserialise(eq("id1"), eq(date), argThat(matchesMap));
        verify(itemDeserialiser).deserialise(eq("id2"), eq(date), argThat(matchesMap));
        verify(itemHandler).handle("id1", date, "theItem1");
        verify(itemHandler).handle("id2", date, "theItem2");
    }

    @Test
    public void forAllWhere() throws Exception {
        LocalDate date = LocalDate.now();
        List<WhereBlob> whereBlobs = Collections.singletonList(WhereBlob.Companion.equals("tag1", new byte[]{}));

        doReturn("theItem").when(itemDeserialiser).deserialise(any(), any(), any());
        itemReader.forAll(date, whereBlobs, itemHandler, itemError);
        verify(blobReader).forAll(eq(tags), eq(whereBlobs), any());

        verify(blobReaderProvider).getReader(date, timeZone);
        ArgumentMatcher<Map<String, byte[]>> matchesMap = eqArrayValueMap(blobReader.getBlobs(tags));
        verify(itemDeserialiser).deserialise(eq("id1"), eq(date), argThat(matchesMap));
        verify(itemHandler).handle("id1", date, "theItem");
    }

    @Test
    public void forAllNullBlobReaderProvider() throws Exception {
        LocalDate date = LocalDate.now();

        doReturn(null).when(blobReaderProvider).getReader(any(), any());
        itemReader.forAll(date, itemHandler, itemError);
        verify(blobReaderProvider).getReader(date, timeZone);
    }

    @Test
    public void handlesBlobReaderException() throws BlobStoreException {
        Set<String> ids = Collections.singleton("id");
        LocalDate date = LocalDate.now();

        BlobStoreException ex = new BlobStoreException("test", null);
        doAnswer(inv -> {throw ex;}).when(blobReader).forEach(anyCollection(), anySet(), any());
        doAnswer(inv -> {throw ex;}).when(blobReader).forAll(anySet(), any());

        itemReader.forEach(ids, date, itemHandler, itemError);
        itemReader.forAll(date, itemHandler, itemError);
        verify(itemError, times(2)).handle(eq(""), eq(date), anyString(), eq(ex));
    }

    @Test
    public void getTag() throws Exception {
        String id = "id1";
        String tag = "tag1";
        LocalDate date = LocalDate.now();

        itemReader = spy(itemReader);
        doReturn("theTag").when(tag1Deserialiser).deserialise(any(), any(), any(), any());
        String result = itemReader.get(id, date, tag, itemError);

        verify(blobReader).get(id, tag);
        byte[] expectedBytes = blobReader.getBlob(tag);
        verify(tag1Deserialiser).deserialise(eq(id), eq(date), eq(tag), eq(expectedBytes));
        assertThat(result, equalTo("theTag"));
    }

    @Test
    public void getTagNullBlobReaderProvider() throws Exception {
        String id = "id1";
        String tag = "tag1";
        LocalDate date = LocalDate.now();

        doReturn(null).when(blobReaderProvider).getReader(any(), any());
        String result = itemReader.get(id, date, tag, itemError);
        assertThat(result, is(nullValue()));
    }

    @Test
    public void forEachTag() throws Exception {
        LocalDate date = LocalDate.now();
        List<String> ids = Collections.singletonList("id1");
        String tag = "tag1";

        doReturn("theTag").when(tag1Deserialiser).deserialise(any(), any(), any(), any());
        itemReader.forEach(ids, date, tag, itemHandler, itemError);
        verify(blobReader).forEach(eq(ids), eq(tag), any());

        verify(blobReaderProvider).getReader(date, timeZone);
        byte[] expectedBytes = blobReader.getBlob(tag);
        verify(tag1Deserialiser).deserialise(eq("id1"), eq(date), eq(tag), eq(expectedBytes));
        verify(itemHandler).handle("id1", date, "theTag");
    }

    @Test
    public void forEachTagNullBlobReaderProvider() throws Exception {
        LocalDate date = LocalDate.now();
        List<String> ids = Collections.singletonList("id1");
        String tag = "tag1";

        doReturn(null).when(blobReaderProvider).getReader(any(), any());
        itemReader.forEach(ids, date, tag, itemHandler, itemError);
        verify(blobReaderProvider).getReader(date, timeZone);
    }

    @Test
    public void forAllTag() throws Exception {
        LocalDate date = LocalDate.now();
        String tag = "tag1";

        doReturn("theTag").when(tag1Deserialiser).deserialise(any(), any(), any(), any());
        itemReader.forAll(date, tag, itemHandler, itemError);
        verify(blobReader).forAll(eq(tag), any());

        verify(blobReaderProvider).getReader(date, timeZone);
        byte[] expectedBytes = blobReader.getBlob(tag);
        verify(tag1Deserialiser).deserialise(eq("id1"), eq(date), eq(tag), eq(expectedBytes));
        verify(tag1Deserialiser).deserialise(eq("id2"), eq(date), eq(tag), eq(expectedBytes));
        verify(itemHandler).handle("id1", date, "theTag");
        verify(itemHandler).handle("id2", date, "theTag");
    }

    @Test
    public void forAllTagNullBlobReaderProvider() throws Exception {
        LocalDate date = LocalDate.now();
        String tag = "tag1";

        doReturn(null).when(blobReaderProvider).getReader(any(), any());
        itemReader.forAll(date, tag, itemHandler, itemError);
        verify(blobReaderProvider).getReader(date, timeZone);
    }

    @Test
    public void getBlobReaderHandlesException() throws BlobStoreException {
        BlobStoreException ex = new BlobStoreException("test", null);
        when(blobReaderProvider.getReader(any(), any())).thenThrow(ex);

        LocalDate date = LocalDate.now();
        itemReader.forEach(Collections.singleton("id"), date, itemHandler, itemError);
        itemReader.forAll(date, itemHandler, itemError);

        itemReader.forEach(Collections.singleton("id"), date, "tag1", itemHandler, itemError);
        itemReader.forAll(date, "tag1", itemHandler, itemError);

        verify(itemError, times(4)).handle(eq(""), eq(date), anyString(), eq(ex));
    }

    @Test
    public void handlesItemDeserialiseException() throws Exception {
        String id = "id1";
        LocalDate date = LocalDate.now();

        DeserialiseException ex = new DeserialiseException("test", null);
        doThrow(ex).when(itemDeserialiser).deserialise(any(), any(), any());
        String str = itemReader.get(id, date, itemError);
        assertNull(str);
        verify(itemError).handle(id, date, "test", ex);
    }

    @Test
    public void handlesTagDeserialiseException() throws Exception {
        String id = "id1";
        LocalDate date = LocalDate.now();

        DeserialiseException ex = new DeserialiseException("test", null);
        doThrow(ex).when(tag1Deserialiser).deserialise(any(), any(), any(), any());
        String str = itemReader.get(id, date, "tag1", itemError);
        assertNull(str);
        verify(itemError).handle(id, date, "test", ex);
    }

}
