/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import java.io.InputStream

/**
 * Allows a ByteBuffer to be used as an InputStream
 */
class ByteArrayBackedInputStream @JvmOverloads constructor(
    private val buffer: ByteArray,
    private var pos: Int = 0,
    length: Int = buffer.size - pos
) : InputStream() {

    private val end = pos + length

    /**
     * Read a single byte from the underlying buffer.
     *
     * @return The byte read, or -1 if there were no more bytes.
     */
    override fun read(): Int =
        if (pos < end)
            buffer[pos++].toInt() and 0xFF
        else
            -1

    /**
     * Read up to the specified number of bytes into the provided array at the specified offset.
     *
     * @param bytes The array to receive the bytes.
     * @param offset The offset in [bytes] to write the new bytes.
     * @param length The maximum number of bytes to read. Check the return value for the actual number of bytes read.
     *
     * @return The actual number of bytes read if the underlying buffer still has bytes available, otherwise -1. The number of bytes read may not match the
     * number requested if the underlying buffer ran out of bytes.
     */
    override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
        if (pos >= end)
            return -1

        // Make sure we do not read beyond the end of the underlying buffer.
        val len = length.coerceAtMost(end - pos)
        System.arraycopy(buffer, pos, bytes, offset, len)
        pos += len

        return len
    }

}
