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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class ItemBlobWriterTest {

    @Mock private BlobWriter blobWriter;
    @Mock private ByDateItemError onError;

    private String id = "id";
    private LocalDate date = LocalDate.now();
    private String tag = "tag";
    private byte[] blob = new byte[]{1, 2, 3, 4};
    private ItemBlobWriter itemWriter;

    @BeforeEach
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void idAndDateAreCorrect() {
        itemWriter = new ItemBlobWriter(blobWriter, id, date, onError);
        assertThat(itemWriter.getId(), is(id));
        assertThat(itemWriter.getDate(), is(date));
    }

    @Test
    public void write() throws Exception {
        doReturn(false).when(blobWriter).update(id, tag, blob, 0, blob.length);
        doReturn(true).when(blobWriter).write(id, tag, blob, 0, blob.length);
        itemWriter = new ItemBlobWriter(blobWriter, id, date, onError);
        assertTrue(itemWriter.write(tag, blob));
        verify(blobWriter).write(id, tag, blob, 0, blob.length);
        assertFalse(itemWriter.anyFailed());
    }

    @Test
    public void update() throws Exception {
        doReturn(true).when(blobWriter).update(id, tag, blob, 0, blob.length);
        itemWriter = new ItemBlobWriter(blobWriter, id, date, onError);
        assertTrue(itemWriter.write(tag, blob));
        verify(blobWriter).update(id, tag, blob, 0, blob.length);
        verify(blobWriter, never()).write(id, tag, blob, 0, blob.length);
        assertFalse(itemWriter.anyFailed());
    }

    @Test
    public void delete() throws Exception {
        itemWriter = new ItemBlobWriter(blobWriter, id, date, onError);
        assertTrue(itemWriter.delete(tag));
        verify(blobWriter).delete(id, tag);
        assertFalse(itemWriter.anyFailed());
    }

    @Test
    public void anyFailed() {
        itemWriter = new ItemBlobWriter(blobWriter, id, date, onError);
        assertFalse(itemWriter.anyFailed());
        assertFalse(itemWriter.write(tag, blob));
        assertTrue(itemWriter.anyFailed());
    }

    @Test
    public void handlesWriteException() throws Exception {
        BlobStoreException ex = new BlobStoreException("", null);
        doThrow(ex).when(blobWriter).update(id, tag, blob, 0, blob.length);

        itemWriter = new ItemBlobWriter(blobWriter, id, date, onError);
        assertFalse(itemWriter.write(tag, blob));
        verify(onError).handle(id, date, "Failed to write", ex);
        assertTrue(itemWriter.anyFailed());
    }

    @Test
    public void handlesDeleteException() throws Exception {
        BlobStoreException ex = new BlobStoreException("", null);
        doThrow(ex).when(blobWriter).delete(id, tag);

        itemWriter = new ItemBlobWriter(blobWriter, id, date, onError);
        assertFalse(itemWriter.delete(tag));
        verify(onError).handle(id, date, "Failed to delete", ex);
        assertTrue(itemWriter.anyFailed());
    }

}
