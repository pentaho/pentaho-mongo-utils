/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.mongo.wrapper;

import org.pentaho.mongo.MongoDbException;

import com.mongodb.DB;

public interface MongoDBAction<ReturnType> {
  public ReturnType perform( DB db ) throws MongoDbException;
}
