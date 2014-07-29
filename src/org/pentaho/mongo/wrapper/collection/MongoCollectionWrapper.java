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

  WriteResult save( DBObject toTry ) throws MongoDbException;

  long count() throws MongoDbException;

  List distinct( String key ) throws MongoDbException;
}
