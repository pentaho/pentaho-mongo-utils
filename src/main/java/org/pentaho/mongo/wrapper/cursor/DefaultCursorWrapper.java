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


package org.pentaho.mongo.wrapper.cursor;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import org.pentaho.mongo.MongoDbException;

public class DefaultCursorWrapper implements MongoCursorWrapper {
  private final DBCursor cursor;

  public DefaultCursorWrapper( DBCursor cursor ) {
    this.cursor = cursor;
  }

  @Override
  public boolean hasNext() throws MongoDbException {
    return cursor.hasNext();
  }

  @Override
  public DBObject next() throws MongoDbException {
    return cursor.next();
  }

  @Override
  public ServerAddress getServerAddress() throws MongoDbException {
    return cursor.getServerAddress();
  }

  @Override
  public void close() throws MongoDbException {
    cursor.close();
  }

  @Override
  public MongoCursorWrapper limit( int i ) throws MongoDbException {
    return wrap( cursor.limit( i ) );
  }

  protected MongoCursorWrapper wrap( DBCursor cursor ) {
    return new DefaultCursorWrapper( cursor );
  }
}
