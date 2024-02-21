/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore.itemwrappers

import com.zepben.blobstore.BlobReader
import com.zepben.blobstore.BlobStoreException
import java.time.LocalDate
import java.time.ZoneId

fun interface ByDateBlobReaderProvider {

    @Throws(BlobStoreException::class)
    fun getReader(profile: LocalDate, timeZone: ZoneId): BlobReader?

}
