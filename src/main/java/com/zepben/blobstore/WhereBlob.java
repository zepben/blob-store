/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import java.util.Arrays;

@EverythingIsNonnullByDefault
public class WhereBlob {

    private final String tag;
    private final byte[] blob;
    private final Type matchType;

    public enum Type {
        EQUAL, NOT_EQUAL
    }

    public static WhereBlob equals(String tag, byte[] blob) {
        return new WhereBlob(tag, blob, Type.EQUAL);
    }

    public static WhereBlob notEqual(String tag, byte[] blob) {
        return new WhereBlob(tag, blob, Type.NOT_EQUAL);
    }

    private WhereBlob(String tag, byte[] blob, Type matchType) {
        this.tag = tag;
        this.blob = Arrays.copyOf(blob, blob.length);
        this.matchType = matchType;
    }

    public String tag() {
        return tag;
    }

    public byte[] blob() {
        return Arrays.copyOf(blob, blob.length);
    }

    public Type matchType() {
        return matchType;
    }

}
