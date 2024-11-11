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


package org.pentaho.mongo.wrapper.collection;

import java.util.List;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

/**
 * Defines the wrapper interface for all interactions with a MongoCollection via
 * a MongoClientWrapper.  All method calls should correspond directly to the
 * call in the underlying MongoCollection, but if appropriate run in the desired
 * AuthContext.
 */
public interface MongoCollectionWrapper {

  MongoCursorWrapper find( DBObject dbObject, DBObject dbObject2 ) throws MongoDbException;

  Cursor aggregate( List<? extends DBObject> pipeline, AggregationOptions options );

  Cursor aggregate( DBObject firstP, DBObject[] remainder, boolean allowDiskUse ) throws MongoDbException;

  MongoCursorWrapper find() throws MongoDbException;

  void drop() throws MongoDbException;

  WriteResult update( DBObject updateQuery, DBObject insertUpdate, boolean upsert, boolean multi )
    throws MongoDbException;

  WriteResult insert( List<DBObject> m_batch ) throws MongoDbException;

  MongoCursorWrapper find( DBObject query ) throws MongoDbException;

  void dropIndex( BasicDBObject mongoIndex ) throws MongoDbException;

  void createIndex( BasicDBObject mongoIndex ) throws MongoDbException;

  void createIndex( BasicDBObject mongoIndex, BasicDBObject options ) throws MongoDbException;

  WriteResult remove() throws MongoDbException;

  WriteResult remove( DBObject query ) throws MongoDbException;

  WriteResult save( DBObject toTry ) throws MongoDbException;

  long count() throws MongoDbException;

  List distinct( String key ) throws MongoDbException;
}
