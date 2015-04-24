/*!
* Copyright 2010 - 2014 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.mongo.wrapper.collection;

import java.util.List;

import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

/**
 * Defines the wrapper interface for all interactions with a MongoCollection via
 * a MongoClientWrapper.  All method calls should correspond directly to the
 * call in the underlying MongoCollection, but if appropriate run in the desired
 * AuthContext.
 */
public interface MongoCollectionWrapper {

  MongoCursorWrapper find( DBObject dbObject, DBObject dbObject2 ) throws MongoDbException;

  AggregationOutput aggregate( DBObject firstP, DBObject[] remainder ) throws MongoDbException;

  MongoCursorWrapper find() throws MongoDbException;

  void drop() throws MongoDbException;

  WriteResult update( DBObject updateQuery, DBObject insertUpdate, boolean upsert, boolean multi )
    throws MongoDbException;

  WriteResult insert( List<DBObject> m_batch ) throws MongoDbException;

  MongoCursorWrapper find( DBObject query ) throws MongoDbException;

  void dropIndex( BasicDBObject mongoIndex ) throws MongoDbException;

  void createIndex( BasicDBObject mongoIndex ) throws MongoDbException;

  WriteResult remove() throws MongoDbException;

  WriteResult remove( DBObject query ) throws MongoDbException;

  WriteResult save( DBObject toTry ) throws MongoDbException;

  long count() throws MongoDbException;

  List distinct( String key ) throws MongoDbException;
}
