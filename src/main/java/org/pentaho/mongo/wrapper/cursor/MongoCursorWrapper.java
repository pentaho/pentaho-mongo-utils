/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.mongo.wrapper.cursor;

import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import org.pentaho.mongo.MongoDbException;

/**
 * Defines the wrapper interface for all interactions with a MongoCursor via
 * a MongoClientWrapper.  All method calls should correspond directly to the
 * call in the underlying MongoCursor, but if appropriate run in the desired
 * AuthContext.
 */
public interface MongoCursorWrapper {

  /**
   *
   * @return true if more elements
   * @throws MongoDbException
   */
  boolean hasNext() throws MongoDbException;

  /**
   * @return the next DBObject
   * @throws MongoDbException
   */
  DBObject next() throws MongoDbException;


  /**
   * @return the server address the cursor is retrieving data from.
   * @throws MongoDbException
   */
  ServerAddress getServerAddress() throws MongoDbException;


  /**
   * closes the cursor
   * @throws MongoDbException
   */
  void close() throws MongoDbException;

  /**
   * @param i the limit to use.  Should be positive.
   * @return a cursor which will will allow iterating over a maximum of i DBObjects.
   * @throws MongoDbException
   */
  MongoCursorWrapper limit( int i ) throws MongoDbException;

}
