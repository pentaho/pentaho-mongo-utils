/*!
* Copyright 2010 - 2017 Hitachi Vantara.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
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
