/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ByteArrayBackedInputStreamTest {

    @Test
    public void readSingleByte() {
        byte[] bytes = new byte[]{1};
        ByteArrayBackedInputStream stream = new ByteArrayBackedInputStream(bytes);
        assertThat(stream.read(), equalTo(1));
        assertThat(stream.read(), equalTo(-1));
    }

    @Test
    public void readArray() {
        byte[] bytes = new byte[]{1, 2, 3, 4, 5, 6};
        ByteArrayBackedInputStream stream = new ByteArrayBackedInputStream(bytes, 1, 4);
        byte[] readBytes = new byte[4];
        int nRead;
        nRead = stream.read(readBytes, 0, 2);
        assertThat(nRead, equalTo(2));

        nRead = stream.read(readBytes, 2, 1);
        assertThat(nRead, equalTo(1));

        nRead = stream.read(readBytes, 3, 2);
        assertThat(nRead, equalTo(1));

        nRead = stream.read(readBytes, 4, 1);
        assertThat(nRead, equalTo(-1));

        byte[] expected = new byte[]{2, 3, 4, 5};
        assertThat(readBytes, equalTo(expected));
    }

}
