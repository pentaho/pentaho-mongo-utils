package org.pentaho.mongo.wrapper.collection;

import java.util.List;

import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

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
}
