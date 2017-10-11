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
