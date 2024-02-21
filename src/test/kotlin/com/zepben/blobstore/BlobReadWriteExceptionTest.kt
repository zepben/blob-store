/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.junit.jupiter.api.Test

class BlobReadWriteExceptionTest {

    @Test
    fun instanceWithThrowable() {
        val error = Error("test")
        val ex = BlobReadWriteException("item1", "test", error)

        assertThat(ex.itemId, equalTo("item1"))
        assertThat(ex.message, equalTo("test"))
        assertThat(ex.cause, equalTo(error))
    }

}
