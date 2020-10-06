/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class BlobReadWriteExceptionTest {

    @Test
    public void instanceWithThrowable() {
        Error error = new Error("test");
        BlobReadWriteException ex = new BlobReadWriteException("item1", "test", error);
        assertThat(ex.itemId(), equalTo("item1"));
        assertThat(ex.getMessage(), equalTo("test"));
        assertThat(ex.getCause(), equalTo(error));
    }

}
