/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import javax.annotation.Nullable;

@EverythingIsNonnullByDefault
public class Captor<T> {

    @Nullable private T capture = null;

    @Nullable
    public T getCapture() {
        return capture;
    }

    public void capture(@Nullable T capture) {
        this.capture = capture;
    }

}
