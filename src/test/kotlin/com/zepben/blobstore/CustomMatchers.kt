/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
package com.zepben.blobstore

import io.mockk.MockKVerificationScope
import java.util.*

object CustomMatchers {

    /**
     * Check that two maps of arrays are equal.
     *
     * NOTE: You can't use the map directly in the verify block as under the covers it will call `equals` on the values, and array1 == array2 will return false
     * even if the contents are equal, unless they are the same reference, so we need to use `Arrays.equals` instead.
     */
    fun MockKVerificationScope.eqArrayValueMap(other: Map<String, ByteArray?>): Map<String, ByteArray?> =
        match { map ->
            (map.size == other.size) &&
                map.all { (key, value) -> Arrays.equals(other[key], value) }
        }

}
