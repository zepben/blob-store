/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.itemwrappers;

import com.zepben.annotations.EverythingIsNonnullByDefault;

@EverythingIsNonnullByDefault
@SuppressWarnings("WeakerAccess")
public class DeserialiseException extends Exception {

    public DeserialiseException(String message) {
        super(message);
    }

    public DeserialiseException(String message, Throwable cause) {
        super(message, cause);
    }

}
