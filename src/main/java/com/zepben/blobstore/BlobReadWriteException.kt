/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

/**
 * Exception that can be thrown when an issue has occurred with a specific item
 */
class BlobReadWriteException(
    val itemId: String,
    message: String? = null,
    cause: Throwable? = null
) : BlobStoreException(message, cause)
