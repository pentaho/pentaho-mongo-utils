/*!
 * Copyright 2010 - 2022 Hitachi Vantara.  All rights reserved.
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

package org.pentaho.mongo.functional;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
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

import static org.junit.Assert.*;

@RunWith( value = Parameterized.class )
public class ClientWrapperIT extends TestBase {

  private final MongoProperties props;

  // how many mongo servers in the cluster?
  private static final int NUM_MONGOS = ( (String) testProperties.get( "multiserver.host" ) ).split( "," ).length;

  private final List<String> tempCollections = new ArrayList<String>();
  private MongoClientWrapper clientWrapper;

  public ClientWrapperIT( MongoProperties props ) {
    this.props = props;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    System.setProperty( "javax.net.ssl.trustStore", ClientWrapperIT.class.getResource( "/keystore.jks" ).getPath() );
  }

  @After
  public void runAfter() throws MongoDbException {
    if ( clientWrapper == null ) {
      clientWrapper = getWrapper( props );
    }
    for ( String tempCollection : tempCollections ) {
      // fail safe drop of tempCollection
      clientWrapper.getCollection( props.get( MongoProp.DBNAME ), tempCollection ).drop();
    }
    tempCollections.clear();

  }

  /**
   * Returns the test data used for each run.  Test methods will be run repeatedly, once for each MongoProperties in the
   * array returned by this method.
   */
  @Parameterized.Parameters
  public static Collection<MongoProperties[]> data() {
    return Arrays.asList( new MongoProperties[][] {
//      { // KERBEROS
//        new MongoProperties.Builder()
//          .set( MongoProp.HOST, (String) testProperties.get( "single.server.host" ) )
//          .set( MongoProp.USERNAME, (String) testProperties.get( "kerberos.user" ) )
//          .set( MongoProp.DBNAME, (String) testProperties.get( "test.db" ) )
//          .set( MongoProp.USE_KERBEROS, "true" ).build() },
//      { // KERBEROS keytab
//        new MongoProperties.Builder()
//          .set( MongoProp.HOST, (String) testProperties.get( "single.server.host" ) )
//          .set( MongoProp.USERNAME, (String) testProperties.get( "kerberos.user" ) )
//          .set( MongoProp.PENTAHO_JAAS_KEYTAB_FILE, (String) testProperties.get( "kerberos.keytab" ) )
//          .set( MongoProp.DBNAME, (String) testProperties.get( "test.db" ) )
//          .set( MongoProp.PENTAHO_JAAS_AUTH_MODE, "KERBEROS_KEYTAB" )
//          .set( MongoProp.USE_KERBEROS, "true" ).build() },
//      { // single server CR
//        new MongoProperties.Builder()
//          .set( MongoProp.HOST, (String) testProperties.get( "single.server.host" ) )
//          .set( MongoProp.PORT, (String) testProperties.get( "userpass.auth.port" ) )
//          .set( MongoProp.USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
//          .set( MongoProp.PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
//          .set(MongoProp.AUTH_DATABASE,(String) testProperties.get("auth.db"))
//          .set(MongoProp.AUTH_MECHA,"SCRAM-SHA-1")
//          .set( MongoProp.DBNAME, (String) testProperties.get( "test.db" ) ).build() },
//      { // multi-server CR
//        new MongoProperties.Builder()
//          .set( MongoProp.HOST, (String) testProperties.get( "multiserver.host" ) )
//          .set( MongoProp.USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
//          .set( MongoProp.PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
//          .set( MongoProp.DBNAME, (String) testProperties.get( "test.db" ) )
//          .set(MongoProp.AUTH_DATABASE,(String) testProperties.get("auth.db"))
//          .set(MongoProp.AUTH_MECHA,"SCRAM-SHA-1")
//          .set( MongoProp.connectionsPerHost, "100" )
//          .set( MongoProp.connectTimeout, "10000" )
//          .set( MongoProp.maxWaitTime, "12000" )
//          .set( MongoProp.readPreference, "primary" )
//          .set( MongoProp.cursorFinalizerEnabled, "true" )
//          .set( MongoProp.socketKeepAlive, "true" )
//          .set( MongoProp.socketTimeout, "30000" ).build() },
//      { // secondary read pref CR
//        new MongoProperties.Builder()
//          .set( MongoProp.HOST, (String) testProperties.get( "multiserver.host" ) )
//          .set( MongoProp.USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
//          .set( MongoProp.PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
//          .set( MongoProp.DBNAME, (String) testProperties.get( "test.db" ) )
//          .set(MongoProp.AUTH_DATABASE,(String) testProperties.get("auth.db"))
//          .set(MongoProp.AUTH_MECHA,"SCRAM-SHA-1")
//          .set( MongoProp.readPreference, "secondary" )
//          .set( MongoProp.writeConcern, Integer.toString( NUM_MONGOS ) )
//          .set( MongoProp.cursorFinalizerEnabled, "true" ).build() },
//      { // secondary read pref CR with tag set1
//        new MongoProperties.Builder()
//          .set( MongoProp.HOST, (String) testProperties.get( "multiserver.host" ) )
//          .set( MongoProp.USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
//          .set( MongoProp.PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
//          .set( MongoProp.DBNAME, (String) testProperties.get( "test.db" ) )
//          .set( MongoProp.readPreference, "secondary" )
//          .set( MongoProp.tagSet, (String) testProperties.get( "tagset1" ) )
//          .set( MongoProp.writeConcern, Integer.toString( NUM_MONGOS ) ).build() },
//      { // secondary read pref CR with tag set2
//        new MongoProperties.Builder()
//          .set( MongoProp.HOST, (String) testProperties.get( "multiserver.host" ) )
//          .set( MongoProp.USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
//          .set( MongoProp.PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
//          .set( MongoProp.DBNAME, (String) testProperties.get( "test.db" ) )
//          .set( MongoProp.readPreference, "secondary" )
//          .set( MongoProp.tagSet, (String) testProperties.get( "tagset2" ) )
//          .set( MongoProp.writeConcern, Integer.toString( NUM_MONGOS ) ).build() },
//      { // SSL turned on
//        new MongoProperties.Builder()
//          .set( MongoProp.HOST, (String) testProperties.get( "ssl.host" ) )
//          .set( MongoProp.USERNAME, (String) testProperties.get( "ssl.user" ) )
//          .set( MongoProp.PASSWORD, (String) testProperties.get( "ssl.password" ) )
//          .set( MongoProp.DBNAME, (String) testProperties.get( "test.db" ) )
//          .set( MongoProp.USE_ALL_REPLICA_SET_MEMBERS, "false" )
//          .set( MongoProp.useSSL, "true" ).build() }
    } );
  }


  @Test
  public void testReadPref() throws MongoDbException {
    // validate the expected server based on read preference specification.
    if ( !props.useAllReplicaSetMembers() && !( props.get( MongoProp.HOST ).split( "," ).length > 1 ) ) {
      // not using replica sets, nothing to validate.
      return;
    }

    final MongoClientWrapper wrapper = getWrapper( props );
    ReplicaSetStatus status = wrapper.getReplicaSetStatus();
    if ( status == null ) {
      // status should be non-null if using replica sets, which will be true
      // if either the property is set to TRUE, or more than one HOST specified.
      fail( "Could not retrieve replica set status with properties:  " + props );
    }

    final MongoCursorWrapper cursor = wrapper.getCollection( props.get( MongoProp.DBNAME ), "inventory" ).find();

    cursor.next();
    ServerAddress readServer = cursor.getServerAddress();

    if ( props.getReadPreference() == ReadPreference.primary() ) {
      assertTrue( "Using primary read preference, but cursor reading from non-primary. \n" + props,
        wrapper.getReplicaSetStatus().getMaster().equals( readServer ) );
    } else if ( props.getReadPreference() == ReadPreference.secondary() ) {

      validateSecondary( wrapper, wrapper.getReplicaSetStatus().getMaster(), readServer );

    }
    wrapper.dispose();
  }

  private void validateSecondary( MongoClientWrapper wrapper, ServerAddress primary, ServerAddress readServer )
    throws MongoDbException {
    final String tagSets = props.get( MongoProp.tagSet );

    if ( tagSets == null ) {
      // don't know for sure what address will be used, but shouldn't be primary.
      assertTrue( "Using secondary read preference, but cursor reading from primary. \n" + props,
        !primary.equals( readServer ) );
    } else {
      // make sure the server used is consistent w/ specified tag set.
      BasicDBList tagSet = (BasicDBList) JSON.parse( "[" + tagSets + "]" );
      List<String> validReplicaSets = wrapper.getReplicaSetMembersThatSatisfyTagSets(
        Arrays.asList( tagSet.toArray( new DBObject[ tagSet.size() ] ) ) );
      List<String> validHosts = extractHostsFromTags( validReplicaSets );

      assertTrue( "Read server not consistent with specified tagSets" + props,
        validHosts.contains( readServer.getHost() + ":" + readServer.getPort() ) );
    }
  }

  private List<String> extractHostsFromTags( List<String> validReplicaSets ) {
    List<String> hosts = new ArrayList<String>();
    for ( String repSet : validReplicaSets ) {
      hosts.add( ( (BasicDBObject) JSON.parse( repSet ) ).get( "host" ).toString() );
    }
    return hosts;
  }


  @Test
  public void testCreateDropCollection() throws MongoDbException {
    if ( props.getReadPreference() != ReadPreference.primary() ) {
      // can't assure secondaries will not be stale at the point of verifying results
      return;
    }
    clientWrapper = getWrapper( props );
    String tempCollection = "testCollection" + UUID.randomUUID().toString().replace( "-", "" );
    tempCollections.add( tempCollection );
    clientWrapper.createCollection( props.get( MongoProp.DBNAME ), tempCollection );
    MongoCollectionWrapper collectionWrapper = clientWrapper.getCollection( props.get( MongoProp.DBNAME ), tempCollection );

    List<DBObject> dbObjects = new ArrayList<DBObject>();
    dbObjects.add( new BasicDBObject( "foo", "bar" ) );
    collectionWrapper.insert( dbObjects );
    assertEquals( props.toString(), "bar", collectionWrapper.find().next().get( "foo" ) );

    assertEquals( props.toString(), 1, collectionWrapper.count() );
    assertEquals( props.toString(), "bar", collectionWrapper.distinct( "foo" ).get( 0 ) );

    collectionWrapper.drop();
    tempCollections.remove( tempCollection );
    assertFalse( props.toString(),
      clientWrapper.getCollectionsNames( props.get( MongoProp.DBNAME ) ).contains( tempCollection ) );
  }

  @Test
  public void testCursor() throws MongoDbException {
    if ( props.getReadPreference() != ReadPreference.primary() ) {
      // can't assure secondaries will not be stale at the point of verifying results
      return;
    }
    clientWrapper = getWrapper( props );

    String tempCollection = "testCollection" + UUID.randomUUID().toString().replace( "-", "" );
    tempCollections.add( tempCollection );
    clientWrapper.createCollection( props.get( MongoProp.DBNAME ), tempCollection );
    MongoCollectionWrapper collectionWrapper = clientWrapper.getCollection(
      props.get( MongoProp.DBNAME ), tempCollection );

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

    clientWrapper.getCollection( props.get( MongoProp.DBNAME ), tempCollection ).drop();
    tempCollections.remove( tempCollection );
    String host = props.get( MongoProp.HOST );
    if ( host != null && !( host.split( "," ).length > 1 ) ) {
      // can't assert a specific host or port if multiple servers specified.
      assertEquals( props.toString(),
        host, cursor.getServerAddress().getHost() );
      assertEquals( props.toString(),
        Integer.parseInt( (String) testProperties.get( "userpass.auth.port" ) ),
        cursor.getServerAddress().getPort() );
    }
  }

  private MongoClientWrapper getWrapper( MongoProperties props ) throws MongoDbException {
    return MongoClientWrapperFactory.createMongoClientWrapper( props, null );
  }
}
