/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.itemwrappers;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import java.time.LocalDate;

/**
 * Handler to handle items once they are read by a {@link ByDateItemReader}.
 */
@FunctionalInterface
@EverythingIsNonnullByDefault
public interface ByDateItemHandler<T> {

    void handle(String id, LocalDate date, T item);

}
