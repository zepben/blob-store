/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import java.io.InputStream;

/**
 * Allows a ByteBuffer to be used as an InputStream
 */
@EverythingIsNonnullByDefault
class ByteArrayBackedInputStream extends InputStream {

    private final byte[] buffer;
    private int pos;
    private final int end;

    ByteArrayBackedInputStream(byte[] buffer) {
        this(buffer, 0, buffer.length);
    }

    ByteArrayBackedInputStream(byte[] buffer, int offset, int length) {
        this.buffer = buffer;
        this.pos = offset;
        this.end = offset + length;
    }

    @Override
    public int read() {
        if (pos >= end) {
            return -1;
        }

        return buffer[pos++] & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        if (pos >= end) {
            return -1;
        }

        length = Math.min(length, end - pos);
        System.arraycopy(buffer, pos, bytes, offset, length);
        pos += length;
        return length;
    }

}
