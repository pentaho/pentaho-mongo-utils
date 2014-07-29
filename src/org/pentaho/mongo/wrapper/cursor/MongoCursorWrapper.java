/*! 
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL 
 * 
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved. 
 * 
 * NOTICE: All information including source code contained herein is, and 
 * remains the sole property of Pentaho and its licensors. The intellectual 
 * and technical concepts contained herein are proprietary and confidential 
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign 
 * patents, or patents in process, and are protected by trade secret and 
 * copyright laws. The receipt or possession of this source code and/or related 
 * information does not convey or imply any rights to reproduce, disclose or 
 * distribute its contents, or to manufacture, use, or sell anything that it 
 * may describe, in whole or in part. Any reproduction, modification, distribution, 
 * or public display of this information without the express written authorization 
 * from Pentaho is strictly prohibited and in violation of applicable laws and 
 * international treaties. Access to the source code contained herein is strictly 
 * prohibited to anyone except those individuals and entities who have executed 
 * confidentiality and non-disclosure agreements or other agreements with Pentaho, 
 * explicitly covering such access. 
 */ 

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
