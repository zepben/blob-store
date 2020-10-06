/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static net.trajano.commons.testing.UtilityClassTestUtil.assertUtilityClassWellDefined;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class BytesUtilTest {

    @Test
    public void isUtilityClass() throws ReflectiveOperationException {
        assertUtilityClassWellDefined(BytesUtil.class);
    }

    @Test
    public void strings() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        String toEncode = "a string";
        BytesUtil.putStringUtf8(buffer, toEncode);
        buffer.flip();
        String decoded = BytesUtil.getStringUtf8(buffer);
        assertThat(decoded, equalTo(toEncode));
    }

    @Test
    public void stringsDirectBuffer() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(128);
        String toEncode = "a string";
        BytesUtil.putStringUtf8(buffer, toEncode);
        buffer.flip();
        String decoded = BytesUtil.getStringUtf8(buffer);
        assertThat(decoded, equalTo(toEncode));
    }

    @Test
    public void int7BitEncodePositive() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        int value = Integer.MAX_VALUE;
        BytesUtil.encode7BitInt(buffer, value);
        buffer.flip();
        int decoded = BytesUtil.decode7BitInt(buffer);
        assertThat(decoded, equalTo(value));
    }

    @Test
    public void int7BitEncodeNegative() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        int value = Integer.MIN_VALUE;
        BytesUtil.encode7BitInt(buffer, value);
        buffer.flip();
        int decoded = BytesUtil.decode7BitInt(buffer);
        assertThat(decoded, equalTo(value));
    }

    @Test
    public void long7BitEncodePositive() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        long value = Long.MAX_VALUE;
        BytesUtil.encode7BitLong(buffer, value);
        buffer.flip();
        long decoded = BytesUtil.decode7BitLong(buffer);
        assertThat(decoded, equalTo(value));
    }

    @Test
    public void long7BitEncodeNegative() {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        long value = Long.MIN_VALUE;
        BytesUtil.encode7BitLong(buffer, value);
        buffer.flip();
        long decoded = BytesUtil.decode7BitLong(buffer);
        assertThat(decoded, equalTo(value));
    }

}
