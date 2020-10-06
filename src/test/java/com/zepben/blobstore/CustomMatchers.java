/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore;

import org.mockito.ArgumentMatcher;

import java.util.Arrays;
import java.util.Map;

public class CustomMatchers {

    public static ArgumentMatcher<Map<String, byte[]>> eqArrayValueMap(Map<String, byte[]> other) {
        return map -> map.size() == other.size() &&
            map.entrySet().stream().allMatch(e -> Arrays.equals(other.get(e.getKey()), e.getValue()));
    }

}
