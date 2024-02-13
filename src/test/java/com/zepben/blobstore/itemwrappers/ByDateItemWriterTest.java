/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.itemwrappers;

import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.BlobWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.function.BiConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class ByDateItemWriterTest {

    @Mock private BlobWriter blobWriter;
    @Mock private ByDateBlobWriterProvider blobWriterProvider;
    @Mock private BiConsumer<ItemBlobWriter, Object> writeHandler;
    @Mock private ByDateItemError itemError;

    @Captor private ArgumentCaptor<ItemBlobWriter> itemBlobWriterCaptor;
    @Captor private ArgumentCaptor<Object> objectCaptor;

    private ZoneId timeZone = ZoneId.systemDefault();
    private ByDateItemWriter itemWriter;

    @BeforeEach
    public void before() {
        MockitoAnnotations.initMocks(this);
        itemWriter = new ByDateItemWriter(timeZone, blobWriterProvider);
    }

    @Test
    public void write() throws BlobStoreException {
        when(blobWriter.write(anyString(), anyString(), any(byte[].class), anyInt(), anyInt())).thenReturn(true);

        when(blobWriterProvider.getWriter(any(), any())).thenReturn(blobWriter);

        String id = "id";
        LocalDate date = LocalDate.now();
        Object item = new Object();

        assertTrue(itemWriter.write("id", LocalDate.now(), item, writeHandler, itemError));
        verify(blobWriterProvider).getWriter(LocalDate.now(), timeZone);
        verify(writeHandler).accept(itemBlobWriterCaptor.capture(), objectCaptor.capture());
        assertThat(objectCaptor.getValue(), is(item));
        assertThat(itemBlobWriterCaptor.getValue().id, is(id));
        assertThat(itemBlobWriterCaptor.getValue().date, is(date));
    }

    @Test
    public void writeHandlesBlobstoreException() throws BlobStoreException {
        BlobStoreException ex = new BlobStoreException("test", null);
        when(blobWriterProvider.getWriter(any(), any())).thenThrow(ex);

        Object item = new Object();
        itemWriter.write("errorId", LocalDate.now(), item, writeHandler, itemError);
        verify(itemError).handle(eq("errorId"), eq(LocalDate.now()), anyString(), eq(ex));
        verify(writeHandler, never()).accept(any(), any());
    }

    @Test
    public void commits() throws BlobStoreException {
        BlobWriter blobWriter1 = mock(BlobWriter.class);
        BlobWriter blobWriter2 = mock(BlobWriter.class);
        when(blobWriterProvider.getWriter(any(), any())).thenReturn(blobWriter1, blobWriter2);

        Object item = new Object();
        itemWriter.write("id", LocalDate.now().minusDays(1), item, writeHandler, itemError);
        itemWriter.write("id", LocalDate.now(), item, writeHandler, itemError);
        assertTrue(itemWriter.commit(itemError));

        verify(blobWriter1, times(1)).commit();
        verify(blobWriter2, times(1)).commit();
    }

    @Test
    public void rollsback() throws BlobStoreException {
        BlobWriter blobWriter1 = mock(BlobWriter.class);
        BlobWriter blobWriter2 = mock(BlobWriter.class);
        when(blobWriterProvider.getWriter(any(), any())).thenReturn(blobWriter1, blobWriter2);

        Object item = new Object();
        itemWriter.write("id", LocalDate.now().minusDays(1), item, writeHandler, itemError);
        itemWriter.write("id", LocalDate.now(), item, writeHandler, itemError);
        assertTrue(itemWriter.rollback(itemError));

        verify(blobWriter1, times(1)).rollback();
        verify(blobWriter2, times(1)).rollback();
    }

}
