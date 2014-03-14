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

package org.pentaho.mongo.wrapper.org.pentaho.functional;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoClientWrapperFactory;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.pentaho.mongo.MongoProp.*;

@RunWith( value = Parameterized.class )
public class ClientWrapperTest extends TestBase {

  private final MongoProperties props;

  public ClientWrapperTest( MongoProperties props ) {
    this.props = props;
  }

  /**
   * Returns the test data used for each run.  Test methods will be run
   * repeatedly, once for each MongoProperties in the array returned by this method.
   */
  @Parameterized.Parameters
  public static Collection<MongoProperties[]> data() {
    return Arrays.asList(new MongoProperties[][] {
      { new MongoProperties.Builder()
        .set( HOST, (String) testProperties.get( "userpass.auth.host" ) )
        .set( PORT, (String) testProperties.get( "userpass.auth.port" ) )
        .set( USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
        .set( PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
        .set( DBNAME, (String) testProperties.get( "userpass.auth.db" ) ).build() },
      { new MongoProperties.Builder()
        .set( HOST, (String) testProperties.get( "multiserver.host" ) )
        .set( USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
        .set( PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
        .set( DBNAME, (String) testProperties.get( "userpass.auth.db" ) )
        .set( connectionsPerHost, "100" )
        .set( connectTimeout, "10000" )
        .set( maxAutoConnectRetryTime, "1000" )
        .set( maxWaitTime, "12000" )
        .set( readPreference, "primary" )
        .set( alwaysUseMBeans, "false" )
        .set( autoConnectRetry, "false" )
        .set( cursorFinalizerEnabled, "true" )
        .set( socketKeepAlive, "false" )
        .set( socketTimeout, "0" ).build() } } );
  }

  @Test
  public void testCreateDropCollection() throws MongoDbException {
    MongoClientWrapper clientWrapper = getWrapper( props );
    String tempCollection = "testCollection" + UUID.randomUUID().toString().replace( "-", "" );

    clientWrapper.createCollection( props.get( DBNAME ), tempCollection );
    MongoCollectionWrapper collectionWrapper = clientWrapper.getCollection( props.get( DBNAME ), tempCollection );

    List<DBObject> dbObjects = new ArrayList<DBObject>();
    dbObjects.add( new BasicDBObject( "foo", "bar" ) );
    collectionWrapper.insert( dbObjects );
    assertEquals( props.toString(), "bar", collectionWrapper.find().next().get( "foo" ) );

    assertEquals( props.toString(), 1, collectionWrapper.count() );
    assertEquals( props.toString(), "bar", collectionWrapper.distinct( "foo" ).get( 0 ) );

    collectionWrapper.drop();
    assertFalse( props.toString(),
      clientWrapper.getCollectionsNames( props.get( DBNAME ) ).contains( tempCollection ) );

    clientWrapper.dispose();
  }

  @Test
  public void testCursor() throws MongoDbException {
    MongoClientWrapper clientWrapper = getWrapper( props );
    String tempCollection = "testCollection" + UUID.randomUUID().toString().replace( "-", "" );

    clientWrapper.createCollection( props.get( DBNAME ), tempCollection );
    MongoCollectionWrapper collectionWrapper = clientWrapper.getCollection(
      props.get( DBNAME ), tempCollection );

    List<DBObject> dbObjects = new ArrayList<DBObject>();
    for ( int i = 0; i < 100; i++ ) {
      dbObjects.add( new BasicDBObject( "foo", "bar" + i ) );
    }
    collectionWrapper.insert( dbObjects );
    MongoCursorWrapper cursor = collectionWrapper.find();
    cursor = cursor.limit( 10 );

    int i = 0;
    while ( cursor.hasNext() ) {
      assertEquals( props.toString(),
        "bar" + i, cursor.next().get( "foo" ) );
      i++;
    }
    assertEquals( "Should be limited to 10 items", 10, i );

    clientWrapper.getCollection( props.get( DBNAME ), tempCollection ).drop();
    String host = props.get( HOST );
    if ( host != null && !( host.split( "," ).length > 1 ) ) {
      // can't assert a specific host or port if multiple servers specified.
      assertEquals( props.toString(),
        host, cursor.getServerAddress().getHost() );
      assertEquals( props.toString(),
        Integer.parseInt( (String) testProperties.get( "userpass.auth.port" ) ),
        cursor.getServerAddress().getPort() );
    }
    clientWrapper.dispose();
  }

  private MongoClientWrapper getWrapper( MongoProperties props ) throws MongoDbException {
    return MongoClientWrapperFactory.createMongoClientWrapper( props, null );
  }
}
