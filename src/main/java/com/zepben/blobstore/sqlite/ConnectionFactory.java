/*
 * Copyright 2020 Zeppelin Bend Pty Ltd
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.zepben.blobstore.sqlite;

import com.zepben.annotations.EverythingIsNonnullByDefault;

import java.sql.Connection;
import java.sql.SQLException;

@EverythingIsNonnullByDefault
public interface ConnectionFactory {

    Connection getConnection() throws SQLException;

}
