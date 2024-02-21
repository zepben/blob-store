/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

import com.zepben.blobstore.BytesUtil.decode7BitInt
import com.zepben.blobstore.BytesUtil.decode7BitLong
import com.zepben.blobstore.BytesUtil.decodeZigZag
import com.zepben.blobstore.BytesUtil.encode7BitInt
import com.zepben.blobstore.BytesUtil.encode7BitLong
import com.zepben.blobstore.BytesUtil.encodeZigZag
import com.zepben.blobstore.BytesUtil.getStringUtf8
import com.zepben.blobstore.BytesUtil.putStringUtf8
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer

class BytesUtilTest {

    @Test
    fun strings() {
        val buffer = ByteBuffer.allocate(128)
        val toEncode = "a string"

        buffer.putStringUtf8(toEncode)
        buffer.flip()

        assertThat(buffer.getStringUtf8(), equalTo(toEncode))
    }

    @Test
    fun stringsDirectBuffer() {
        val buffer = ByteBuffer.allocateDirect(128)
        val toEncode = "a string"

        buffer.putStringUtf8(toEncode)
        buffer.flip()

        assertThat(buffer.getStringUtf8(), equalTo(toEncode))
    }

    @Test
    fun int7BitEncodePositive() {
        val buffer = ByteBuffer.allocate(128)
        val value = Int.MAX_VALUE

        buffer.encode7BitInt(value)
        buffer.flip()


        assertThat(decode7BitInt(buffer), equalTo(value))
    }

    @Test
    fun int7BitEncodeNegative() {
        val buffer = ByteBuffer.allocate(128)
        val value = Int.MIN_VALUE

        buffer.encode7BitInt(value)
        buffer.flip()

        assertThat(decode7BitInt(buffer), equalTo(value))
    }

    @Test
    fun long7BitEncodePositive() {
        val buffer = ByteBuffer.allocate(128)
        val value = Long.MAX_VALUE

        encode7BitLong(buffer, value)
        buffer.flip()

        assertThat(decode7BitLong(buffer), equalTo(value))
    }

    @Test
    fun long7BitEncodeNegative() {
        val buffer = ByteBuffer.allocate(128)
        val value = Long.MIN_VALUE

        encode7BitLong(buffer, value)
        buffer.flip()

        assertThat(decode7BitLong(buffer), equalTo(value))
    }


    @Test
    fun intZigzagEncodePositive() {
        val value = Int.MAX_VALUE

        val encoded = encodeZigZag(value)

        assertThat(decodeZigZag(encoded), equalTo(value))
    }

    @Test
    fun intZigzagEncodeNegative() {
        val value = Int.MIN_VALUE

        val encoded = encodeZigZag(value)

        assertThat(decodeZigZag(encoded), equalTo(value))
    }

    @Test
    fun longZigzagEncodePositive() {
        val value = Long.MIN_VALUE

        val encoded = encodeZigZag(value)

        assertThat(decodeZigZag(encoded), equalTo(value))
    }

    @Test
    fun longZigzagEncodeNegative() {
        val value = Long.MIN_VALUE

        val encoded = encodeZigZag(value)

        assertThat(decodeZigZag(encoded), equalTo(value))
    }

}
