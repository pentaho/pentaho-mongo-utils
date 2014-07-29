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
import org.pentaho.mongo.wrapper.cursor.DefaultCursorWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

public class DefaultMongoCollectionWrapper implements MongoCollectionWrapper {
  private final DBCollection collection;

  public DefaultMongoCollectionWrapper( DBCollection collection ) {
    this.collection = collection;
  }

  @Override
  public MongoCursorWrapper find( DBObject dbObject, DBObject dbObject2 ) throws MongoDbException {
    return wrap( collection.find( dbObject, dbObject2 ) );
  }

  @Override
  public AggregationOutput aggregate( DBObject firstP, DBObject[] remainder ) throws MongoDbException {
    return collection.aggregate( firstP, remainder );
  }

  @Override
  public MongoCursorWrapper find() throws MongoDbException {
    return wrap( collection.find() );
  }

  @Override
  public void drop() throws MongoDbException {
    collection.drop();
  }

  @Override
  public WriteResult update( DBObject updateQuery, DBObject insertUpdate, boolean upsert, boolean multi )
    throws MongoDbException {
    return collection.update( updateQuery, insertUpdate, upsert, multi );
  }

  @Override
  public WriteResult insert( List<DBObject> m_batch ) throws MongoDbException {
    return collection.insert( m_batch );
  }

  @Override
  public MongoCursorWrapper find( DBObject query ) throws MongoDbException {
    return wrap( collection.find( query ) );
  }

  @Override
  public void dropIndex( BasicDBObject mongoIndex ) throws MongoDbException {
    collection.dropIndex( mongoIndex );
  }

  @Override
  public void createIndex( BasicDBObject mongoIndex ) throws MongoDbException {
    collection.createIndex( mongoIndex );
  }

  @Override
  public WriteResult save( DBObject toTry ) throws MongoDbException {
    return collection.save( toTry );
  }

  @Override public long count() throws MongoDbException {
    return collection.count();
  }

  @Override public List distinct( String key ) throws MongoDbException {
    return collection.distinct( key );
  }

  protected MongoCursorWrapper wrap( DBCursor cursor ) {
    return new DefaultCursorWrapper( cursor );
  }
}
