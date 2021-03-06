/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.itemwrappers;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import javax.annotation.Nullable;
import java.time.LocalDate;

@EverythingIsNonnullByDefault
@FunctionalInterface
public interface ByDateTagDeserialiser<T> {

    @Nullable
    T deserialise(String id, LocalDate date, String tag, byte[] blob) throws DeserialiseException;

}
