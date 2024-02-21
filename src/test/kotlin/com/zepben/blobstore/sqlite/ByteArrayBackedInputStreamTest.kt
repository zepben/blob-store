/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.sqlite

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Test

class ByteArrayBackedInputStreamTest {

    @Test
    fun readSingleByte() {
        val bytes = byteArrayOf(1)
        ByteArrayBackedInputStream(bytes).use { stream ->
            assertThat(stream.read(), equalTo(1))
            assertThat(stream.read(), equalTo(-1))
        }
    }

    @Test
    fun readArray() {
        val bytes = byteArrayOf(1, 2, 3, 4, 5, 6)
        val readBytes = ByteArray(4)

        // We are starting the stream from byte 1, so the expected should start from `2`.
        ByteArrayBackedInputStream(bytes, 1, 4).use { stream ->
            assertThat(stream.read(readBytes, 0, 2), equalTo(2))
            assertThat(stream.read(readBytes, 2, 1), equalTo(1))

            // Should only read 1 byte as the read buffer should be full.
            assertThat(stream.read(readBytes, 3, 2), equalTo(1))
            assertThat(stream.read(readBytes, 4, 1), equalTo(-1))

            // Should have started from `2` and not had enough room for the `6`
            assertThat(readBytes, equalTo(byteArrayOf(2, 3, 4, 5)))
        }
    }

}
