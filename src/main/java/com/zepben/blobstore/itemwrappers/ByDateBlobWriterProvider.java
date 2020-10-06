/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.itemwrappers;

import com.zepben.annotations.EverythingIsNonnullByDefault;
import com.zepben.blobstore.BlobStoreException;
import com.zepben.blobstore.BlobWriter;

import java.time.LocalDate;
import java.time.ZoneId;

@EverythingIsNonnullByDefault
public interface ByDateBlobWriterProvider {

    BlobWriter getWriter(LocalDate date, ZoneId timeZone) throws BlobStoreException;

}
