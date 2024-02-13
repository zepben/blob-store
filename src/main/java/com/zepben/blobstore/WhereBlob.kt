/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

class WhereBlob private constructor(
    val tag: String,
    blob: ByteArray,
    val matchType: Type
) {

    // We take a copy of the passed array, so we are not affected by any modifications.
    val blob: ByteArray = blob.copyOf(blob.size)
        // We return a copy of the blob so no one else can modify it.
        get() = field.copyOf(field.size)

    enum class Type(val operator: String) {
        EQUAL("="),
        NOT_EQUAL("<>")
    }

    companion object {

        fun equals(tag: String, blob: ByteArray): WhereBlob =
            WhereBlob(tag, blob, Type.EQUAL)

        fun notEqual(tag: String, blob: ByteArray): WhereBlob =
            WhereBlob(tag, blob, Type.NOT_EQUAL)

    }

}
