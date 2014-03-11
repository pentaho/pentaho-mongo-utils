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

  protected MongoCursorWrapper wrap( DBCursor cursor ) {
    return new DefaultCursorWrapper( cursor );
  }
}
