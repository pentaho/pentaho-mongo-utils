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
import java.util.ArrayList;
import java.util.Collections;

import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.cursor.DefaultCursorWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
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
  public Cursor aggregate( List<? extends DBObject> pipeline, AggregationOptions options ) {
    return collection.aggregate( pipeline, options );
  }

  @Override
  public Cursor aggregate( DBObject firstP, DBObject[] remainder, boolean allowDiskUse ) {
    AggregationOptions options = AggregationOptions.builder().allowDiskUse( allowDiskUse ).build();
    List<DBObject> pipeline = new ArrayList<>();
    pipeline.add( firstP );
    Collections.addAll( pipeline, remainder );
    return aggregate( pipeline, options );
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
  public void createIndex( BasicDBObject mongoIndex, BasicDBObject options ) throws MongoDbException {
    collection.createIndex( mongoIndex, options );
  }

  @Override
  public WriteResult remove() throws MongoDbException {
    return remove( new BasicDBObject() );
  }

  @Override
  public WriteResult remove( DBObject query ) throws MongoDbException {
    return collection.remove( query );
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
