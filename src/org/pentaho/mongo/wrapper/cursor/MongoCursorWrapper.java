package org.pentaho.mongo.wrapper.cursor;

import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import org.pentaho.mongo.MongoDbException;

public interface MongoCursorWrapper {

  boolean hasNext() throws MongoDbException;

  DBObject next() throws MongoDbException;

  ServerAddress getServerAddress() throws MongoDbException;

  void close() throws MongoDbException;

  MongoCursorWrapper limit( int i ) throws MongoDbException;

}
