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
import static org.pentaho.mongo.MongoProp.*;

@RunWith( value = Parameterized.class )
public class ClientWrapperTest extends TestBase {

  private final MongoProperties props;

  // how many mongo servers in the cluster?
  private static final int NUM_MONGOS = ( (String) testProperties.get( "multiserver.host" ) ).split( "," ).length;

  private final List<String> tempCollections = new ArrayList<String>();
  private MongoClientWrapper clientWrapper;
  public ClientWrapperTest( MongoProperties props ) {
    this.props = props;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    System.setProperty( "javax.net.ssl.trustStore", "keystore.jks" );
  }

  @After
  public void runAfter() throws MongoDbException {
    if ( clientWrapper == null ) {
      clientWrapper = getWrapper( props );
    }
    for ( String tempCollection : tempCollections ) {
      // fail safe drop of tempCollection
      clientWrapper.getCollection( props.get( DBNAME ), tempCollection ).drop();
    }
    tempCollections.clear();

  }

  /**
   * Returns the test data used for each run.  Test methods will be run
   * repeatedly, once for each MongoProperties in the array returned by this method.
   */
  @Parameterized.Parameters
  public static Collection<MongoProperties[]> data() {
    return Arrays.asList(new MongoProperties[][] {
      { // KERBEROS
        new MongoProperties.Builder()
          .set( HOST, (String) testProperties.get( "single.server.host" ) )
          .set( USERNAME, (String) testProperties.get( "kerberos.user" ) )
          .set( DBNAME, (String) testProperties.get( "test.db" ) )
          .set( USE_KERBEROS, "true" ).build() } ,
      { // KERBEROS keytab
        new MongoProperties.Builder()
          .set( HOST, (String) testProperties.get( "single.server.host" ) )
          .set( USERNAME, (String) testProperties.get( "kerberos.user" ) )
          .set( PENTAHO_JAAS_KEYTAB_FILE, (String) testProperties.get( "kerberos.keytab" ) )
          .set( DBNAME, (String) testProperties.get( "test.db" ) )
          .set( PENTAHO_JAAS_AUTH_MODE, "KERBEROS_KEYTAB" )
          .set( USE_KERBEROS, "true" ).build() } ,
      { // single server CR
        new MongoProperties.Builder()
          .set( HOST, (String) testProperties.get( "single.server.host" ) )
          .set( PORT, (String) testProperties.get( "userpass.auth.port" ) )
          .set( USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
          .set( PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
          .set( DBNAME, (String) testProperties.get( "test.db" ) ).build() },
      { // multi-server CR
        new MongoProperties.Builder()
          .set( HOST, (String) testProperties.get( "multiserver.host" ) )
          .set( USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
          .set( PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
          .set( DBNAME, (String) testProperties.get( "test.db" ) )
          .set( connectionsPerHost, "100" )
          .set( connectTimeout, "10000" )
          .set( maxWaitTime, "12000" )
          .set( readPreference, "primary" )
          .set( cursorFinalizerEnabled, "true" )
          .set( socketKeepAlive, "false" )
          .set( socketTimeout, "0" ).build() },
      { // secondary read pref CR
        new MongoProperties.Builder()
          .set( HOST, (String) testProperties.get( "multiserver.host" ) )
          .set( USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
          .set( PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
          .set( DBNAME, (String) testProperties.get( "test.db" ) )
          .set( readPreference, "secondary" )
          .set( writeConcern, Integer.toString( NUM_MONGOS ) )
          .set( cursorFinalizerEnabled, "true" ).build() } ,
      { // secondary read pref CR with tag set1
        new MongoProperties.Builder()
          .set( HOST, (String) testProperties.get( "multiserver.host" ) )
          .set( USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
          .set( PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
          .set( DBNAME, (String) testProperties.get( "test.db" ) )
          .set( readPreference, "secondary" )
          .set( tagSet, (String) testProperties.get( "tagset1" ) )
          .set( writeConcern, Integer.toString( NUM_MONGOS ) ).build() } ,
      { // secondary read pref CR with tag set2
        new MongoProperties.Builder()
          .set( HOST, (String) testProperties.get( "multiserver.host" ) )
          .set( USERNAME, (String) testProperties.get( "userpass.auth.user" ) )
          .set( PASSWORD, (String) testProperties.get( "userpass.auth.password" ) )
          .set( DBNAME, (String) testProperties.get( "test.db" ) )
          .set( readPreference, "secondary" )
          .set( tagSet, (String) testProperties.get( "tagset2" ) )
          .set( writeConcern, Integer.toString( NUM_MONGOS ) ).build() } ,
      { // SSL turned on
        new MongoProperties.Builder()
          .set( HOST, (String) testProperties.get( "ssl.host" ) )
          .set( USERNAME, (String) testProperties.get( "ssl.user" ) )
          .set( PASSWORD, (String) testProperties.get( "ssl.password" ) )
          .set( DBNAME, (String) testProperties.get( "test.db" ) )
          .set( USE_ALL_REPLICA_SET_MEMBERS, "false" )
          .set( useSSL, "true" ).build() }
    } );
  }


  @Test
  public void testReadPref() throws MongoDbException {
    // validate the expected server based on read preference specification.
    if ( !props.useAllReplicaSetMembers() && !( props.get( HOST ).split( "," ).length > 1 ) ) {
      // not using replica sets, nothing to validate.
      return;
    }

    final MongoClientWrapper wrapper = getWrapper( props );
    ReplicaSetStatus status = wrapper.getReplicaSetStatus();
    if ( status == null ) {
      // status should be non-null if using replica sets, which will be true
      // if either the property is set to TRUE, or more than one HOST specified.
      fail( "Could not retrieve replica set status with properties:  "  + props );
    }
    final ServerAddress primary = status.getMaster();
    final MongoCursorWrapper cursor = wrapper.getCollection( props.get( DBNAME ), "sales" ).find();

    cursor.next();
    ServerAddress readServer = cursor.getServerAddress();

    if ( props.getReadPreference() == ReadPreference.primary() ) {
      assertTrue( "Using primary read preference, but cursor reading from non-primary. \n" + props,
        primary.equals( readServer ) );
    } else if ( props.getReadPreference() == ReadPreference.secondary() ) {

      validateSecondary( wrapper, primary, readServer );

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
    clientWrapper.createCollection( props.get( DBNAME ), tempCollection );
    MongoCollectionWrapper collectionWrapper = clientWrapper.getCollection( props.get( DBNAME ), tempCollection );

    List<DBObject> dbObjects = new ArrayList<DBObject>();
    dbObjects.add( new BasicDBObject( "foo", "bar" ) );
    collectionWrapper.insert( dbObjects );
    assertEquals( props.toString(), "bar", collectionWrapper.find().next().get( "foo" ) );

    assertEquals( props.toString(), 1, collectionWrapper.count() );
    assertEquals( props.toString(), "bar", collectionWrapper.distinct( "foo" ).get( 0 ) );

    collectionWrapper.drop();
    tempCollections.remove( tempCollection );
    assertFalse( props.toString(),
      clientWrapper.getCollectionsNames( props.get( DBNAME ) ).contains( tempCollection ) );
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
    tempCollections.remove( tempCollection );
    String host = props.get( HOST );
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
