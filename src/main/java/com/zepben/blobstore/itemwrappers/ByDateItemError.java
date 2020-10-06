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

/**
 * Handler for when there is an error reading or writing items with {@link ByDateItemReader} and {@link ByDateItemWriter}
 */
@FunctionalInterface
@EverythingIsNonnullByDefault
public interface ByDateItemError {

    void handle(String id,
                LocalDate date,
                String msg,
                @Nullable Throwable t);

}
