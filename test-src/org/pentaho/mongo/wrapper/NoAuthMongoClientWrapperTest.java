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

package org.pentaho.mongo.wrapper;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.mongo.BaseMessages;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.Mockito.*;

public class NoAuthMongoClientWrapperTest {


  public static String REP_SET_CONFIG = "{\"_id\" : \"foo\", \"version\" : 1, " + "\"members\" : [" + "{"
      + "\"_id\" : 0, " + "\"host\" : \"palladium.lan:27017\", " + "\"tags\" : {" + "\"dc.one\" : \"primary\", "
      + "\"use\" : \"production\"" + "}" + "}, " + "{" + "\"_id\" : 1, " + "\"host\" : \"palladium.local:27018\", "
      + "\"tags\" : {" + "\"dc.two\" : \"slave1\"" + "}" + "}, " + "{" + "\"_id\" : 2, "
      + "\"host\" : \"palladium.local:27019\", " + "\"tags\" : {" + "\"dc.three\" : \"slave2\", "
      + "\"use\" : \"production\"" + "}" + "}" + "]," + "\"settings\" : {" + "\"getLastErrorModes\" : { "
      + "\"DCThree\" : {" + "\"dc.three\" : 1" + "}" + "}" + "}" + "}";

  private static final String TAG_SET = "{\"use\" : \"production\"}";
  private static final String TAG_SET2 = "{\"use\" : \"ops\"}";
  private static final String TAG_SET_LIST = "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"a\" },"
      + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"d\" },"
      + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"mem\": \"r\"}";


  @Mock private MongoClient mockMongoClient;
  @Mock private MongoUtilLogger mockMongoUtilLogger;
  @Mock private MongoProperties mongoProperties;
  @Mock private MongoClientFactory mongoClientFactory;
  @Mock private MongoClientOptions mongoClientOptions;
  @Mock private DB mockDB;
  @Mock DBCollection collection;
  @Mock private RuntimeException runtimeException;
  @Captor private ArgumentCaptor<List<ServerAddress>> serverAddresses;
  @Captor private ArgumentCaptor<List<MongoCredential>> mongoCredentials;

  private NoAuthMongoClientWrapper noAuthMongoClientWrapper;

  private static Class<?> PKG = NoAuthMongoClientWrapper.class;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks( this );
    when( mongoClientFactory.getMongoClient( anyList(), anyList(),
        any( MongoClientOptions.class ), anyBoolean() ) )
        .thenReturn( mockMongoClient );
    NoAuthMongoClientWrapper.clientFactory = mongoClientFactory;
    noAuthMongoClientWrapper = new NoAuthMongoClientWrapper(
        mockMongoClient, mongoProperties,
        mockMongoUtilLogger );
  }

  @Test
  public void testPerform() throws Exception {
    MongoDBAction mockMongoDBAction = mock( MongoDBAction.class );
    noAuthMongoClientWrapper.perform( "Test", mockMongoDBAction );
    verify( mockMongoDBAction, times( 1 ) ).perform( noAuthMongoClientWrapper.getDb( "Test" ) );
  }

  @Test
  public void testGetLastErrorMode() throws MongoDbException {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    DBCollection dbCollection = mock( DBCollection.class );
    when( dbCollection.findOne() ).thenReturn( config );
    when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) )
        .thenReturn( mockDB );
    when( mockDB.getCollection( NoAuthMongoClientWrapper.REPL_SET_COLLECTION ) )
        .thenReturn( dbCollection );

    assertThat( noAuthMongoClientWrapper.getLastErrorModes(), equalTo( Arrays.asList( "DCThree" ) ) );
  }


  @Test
  public void testGetAllReplicaSetMemberRecords() {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( NoAuthMongoClientWrapper.REPL_SET_MEMBERS );

    assertTrue( members != null );
    assertTrue( members instanceof BasicDBList );
    assertEquals( 3, ( (BasicDBList) members ).size() );
  }

  @Test
  public void testSetupAllTags() {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( NoAuthMongoClientWrapper.REPL_SET_MEMBERS );

    List<String> allTags = noAuthMongoClientWrapper.setupAllTags( (BasicDBList) members );

    assertEquals( 4, allTags.size() );
  }

  @Test
  public void testGetReplicaSetMembersThatSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();

    List<DBObject> tagSets = new ArrayList<DBObject>(); // tags to satisfy

    DBObject tSet = (DBObject) JSON.parse( TAG_SET );
    tagSets.add( tSet );

    List<String> satisfy =
        noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets( tagSets );
    // two replica set members have the "use : production" tag in their tag sets
    assertEquals( 2, satisfy.size() );
    assertThat( satisfy.get( 0 ), containsString( "palladium.lan:27017" ) );
    assertThat( satisfy.get( 1 ), containsString( "palladium.local:27019" ) );
  }

  @Test
  public void testGetReplicaSetMembersBadInput() throws MongoDbException {
    setupMockedReplSet();
    try {
      noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets( null );
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( MongoDbException.class ) );
    }
  }

  @Test
  public void testGetReplicaSetMembersDoesntSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();
    List<DBObject> tagSets = new ArrayList<DBObject>(); // tags to satisfy
    DBObject tSet = (DBObject) JSON.parse( TAG_SET2 );
    tagSets.add( tSet );
    List<String> satisfy =
        noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets( tagSets );
    // no replica set members have the "use : ops" tag in their tag sets
    assertEquals( 0, satisfy.size() );
  }


  @Test
  public void testGetReplicaSetMembersThatSatisfyTagSetsThrowsOnDbError() throws MongoDbException {
    setupMockedReplSet();
    List<DBObject> tagSets = new ArrayList<DBObject>(); // tags to satisfy
    DBObject tSet = (DBObject) JSON.parse( TAG_SET );
    tagSets.add( tSet );
    doThrow( runtimeException ).when( mockMongoClient )
        .getDB( NoAuthMongoClientWrapper.LOCAL_DB );
    try {
      noAuthMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets( tagSets );
      fail( "expected exception." );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( MongoDbException.class ) );
    }
  }

  @Test
  public void testGetClientStandalone() throws MongoDbException, UnknownHostException {
    ServerAddress address = new ServerAddress( "fakehost", 1010 );
    when( mongoProperties.get( MongoProp.HOST ) ).thenReturn( address.getHost() );
    when( mongoProperties.get( MongoProp.PORT ) ).thenReturn( "1010" );
    when( mongoProperties.useAllReplicaSetMembers() ).thenReturn( false );

    noAuthMongoClientWrapper.getClient( mongoClientOptions );

    verify( mongoClientFactory ).getMongoClient( serverAddresses.capture(), mongoCredentials.capture(),
        any( MongoClientOptions.class ), eq( false ) );

    assertThat( serverAddresses.getValue(), equalTo( Arrays.asList( address ) ) );
    assertThat( "No credentials should be associated w/ NoAuth", mongoCredentials.getValue().size(), equalTo( 0 ) );
  }

  @Test
  public void testGetClientRepSet() throws MongoDbException, UnknownHostException {
    when( mongoProperties.get( MongoProp.HOST ) ).thenReturn( "host1:1010,host2:2020,host3:3030" );
    when( mongoProperties.useAllReplicaSetMembers() ).thenReturn( true );
    noAuthMongoClientWrapper.getClient( mongoClientOptions );

    verify( mongoClientFactory ).getMongoClient( serverAddresses.capture(), mongoCredentials.capture(),
        eq( mongoClientOptions ), eq( true ) );

    List<ServerAddress> addresses = Arrays.asList(
        new ServerAddress( "host1", 1010 ),
        new ServerAddress( "host2", 2020 ),
        new ServerAddress( "host3", 3030 ) );
    assertThat( serverAddresses.getValue(), equalTo( addresses ) );
  }

  @Test
  public void testBadHostsAndPorts() throws MongoDbException, UnknownHostException {
    when( mongoProperties.get( MongoProp.HOST ) ).thenReturn( "host1:1010:2020" );
    try {
      noAuthMongoClientWrapper.getClient( mongoClientOptions );
      fail( "expected exception:  malformed host " );
    } catch ( MongoDbException e ) {
    }

    when( mongoProperties.get( MongoProp.HOST ) ).thenReturn( "" );
    try {
      noAuthMongoClientWrapper.getClient( mongoClientOptions );
      fail( "expected exception:  empty host string " );
    } catch ( MongoDbException e ) {
    }

    when( mongoProperties.get( MongoProp.HOST ) ).thenReturn( "   " );
    try {
      noAuthMongoClientWrapper.getClient( mongoClientOptions );
      fail( "expected exception:  empty host string " );
    } catch ( MongoDbException e ) {
    }

    when( mongoProperties.get( MongoProp.HOST ) ).thenReturn( "host1:badport" );
    try {
      noAuthMongoClientWrapper.getClient( mongoClientOptions );
      fail( "expected exception:  bad port " );
    } catch ( MongoDbException e ) {
    }
  }

  @Test
  public void testConstructorInitializesClientWithProps() throws MongoDbException {
    final AtomicBoolean clientCalled = new AtomicBoolean( false );
    final MongoClientOptions options = mock( MongoClientOptions.class );
    when( mongoProperties.buildMongoClientOptions( mockMongoUtilLogger ) )
        .thenReturn( options );
    new NoAuthMongoClientWrapper( mongoProperties, mockMongoUtilLogger ) {
      protected MongoClient getClient( MongoClientOptions opts ) {
        clientCalled.set( true );
        assertThat( opts, equalTo( options ) );
        return null;
      } };
    verify( mongoProperties ).buildMongoClientOptions( mockMongoUtilLogger );
    assertTrue( clientCalled.get() );
  }

  @Test
  public void operationsDelegateToMongoClient() throws MongoDbException {
    noAuthMongoClientWrapper.getDatabaseNames();
    verify( mockMongoClient ).getDatabaseNames();

    noAuthMongoClientWrapper.getDb( "foo" );
    verify( mockMongoClient ).getDB( "foo" );

    when( mockMongoClient.getDB( "foo" ) ).thenReturn( mockDB );
    noAuthMongoClientWrapper.getCollectionsNames( "foo" );
    verify( mockDB ).getCollectionNames();
  }

  @Test
  public void mongoExceptionsPropogate() {
    doThrow( runtimeException ).when( mockMongoClient ).getDatabaseNames();
    try {
      noAuthMongoClientWrapper.getDatabaseNames();
      fail( "expected exception" );
    } catch ( Exception mde ) {
      assertThat( mde, instanceOf( MongoDbException.class ) );
    }
    doThrow( runtimeException ).when( mockMongoClient ).getDB( "foo" );
    try {
      noAuthMongoClientWrapper.getDb( "foo" );
      fail( "expected exception" );
    } catch ( Exception mde ) {
      assertThat( mde, instanceOf( MongoDbException.class ) );
    }

  }

  @Test
  public void mongoGetCollNamesExceptionPropgates() {
    when( mockMongoClient.getDB( "foo" ) ).thenReturn( mockDB );
    doThrow( runtimeException ).when( mockDB ).getCollectionNames();
    try {
      noAuthMongoClientWrapper.getCollectionsNames( "foo" );
      fail( "expected exception" );
    } catch ( Exception mde ) {
      assertThat( mde, instanceOf( MongoDbException.class ) );
    }
  }

  @Test
  public void testGetIndex() throws MongoDbException {
    when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    when( mockDB.collectionExists( "collection" ) ).thenReturn( true );
    DBCollection collection = mock( DBCollection.class );
    DBObject indexInfoObj = mock( DBObject.class );
    when( indexInfoObj.toString() ).thenReturn( "indexInfo" );
    List<DBObject> indexInfo = Arrays.asList( indexInfoObj );
    when( collection.getIndexInfo() ).thenReturn( indexInfo );
    when( mockDB.getCollection( "collection" ) ).thenReturn( collection );

    assertThat( noAuthMongoClientWrapper.getIndexInfo( "fakeDb", "collection" ),
        equalTo( Arrays.asList( "indexInfo" ) ) );
  }

  @Test
  public void testGetIndexCollectionDoesntExist() throws MongoDbException {
    when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    when( mockDB.collectionExists( "collection" ) ).thenReturn( false );
    DBCollection collection = mock( DBCollection.class );
    DBObject indexInfoObj = mock( DBObject.class );
    when( indexInfoObj.toString() ).thenReturn( "indexInfo" );
    List<DBObject> indexInfo = Arrays.asList( indexInfoObj );
    when( collection.getIndexInfo() ).thenReturn( indexInfo );
    when( mockDB.getCollection( "collection" ) ).thenReturn( collection );
    assertThat( noAuthMongoClientWrapper.getIndexInfo( "fakeDb", "collection" ),
        equalTo( Arrays.asList( "indexInfo" ) ) );
    verify( mockDB ).createCollection( "collection", null );
  }

  @Test
  public void testGetIndexCollectionNotSpecified() throws MongoDbException {
    when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    try {
      noAuthMongoClientWrapper.getIndexInfo( "fakeDb", "" );
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( MongoDbException.class ) );
    }
  }

  @Test
  public void getIndexInfoErrorConditions() {
    try {
      noAuthMongoClientWrapper.getIndexInfo( "fakeDb", "collection" );
      fail( "expected exception since DB is null" );
    } catch ( Exception e ) {
    }
    try {
      noAuthMongoClientWrapper.getIndexInfo( "fakeDb", "" );
      fail( "expected exception since no collection specified." );
    } catch ( Exception e ) {
    }
    when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    when( mockDB.collectionExists( "collection" ) ).thenReturn( true );
    try {
      noAuthMongoClientWrapper.getIndexInfo( "fakeDb", "collection" );
      fail( "expected exception since null collection" );
    } catch ( Exception e ) {
      verify( mockDB ).getCollection( "collection" );
      assertThat( e.getMessage(),
          containsString( BaseMessages.getString( PKG,
              "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection",
              "collection" ) ) );
    }
  }

  @Test
  public void testGetIndexNoIndexThrows() {
    initFakeDb();
    try {
      noAuthMongoClientWrapper.getIndexInfo( "fakeDb", "collection" );
      fail( "expected exception since no index info" );
    } catch ( Exception e ) {
      verify( mockDB ).getCollection( "collection" );
      assertThat( e.getMessage(),
          containsString( BaseMessages.getString( PKG,
              "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection",
              "collection" ) ) );
    }
  }

  private void initFakeDb() {
    when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    when( mockDB.collectionExists( "collection" ) ).thenReturn( true );
    when( mockDB.getCollection( "collection" ) ).thenReturn( collection );
  }

  @Test
  public void testGetAllTagsNoDB() throws MongoDbException {
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    verify( mockMongoUtilLogger ).info(
        BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Warning.LocalDBNotAvailable" ) );
    assertThat( tags.size(), equalTo( 0 ) );
  }

  @Test
  public void testGetAllTagsNoRepSet() throws MongoDbException {
    when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    verify( mockMongoUtilLogger ).info(
        BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.Message.Warning.ReplicaSetCollectionUnavailable" ) );
    assertThat( tags.size(), equalTo( 0 ) );
  }

  @Test
  public void testGetAllTagsRepSetEmtpy() throws MongoDbException {
    when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    when( mockDB.getCollection( NoAuthMongoClientWrapper.REPL_SET_COLLECTION ) )
        .thenReturn( collection );
    DBObject membersList = new BasicDBList();
    DBObject basicDBObject = new BasicDBObject( NoAuthMongoClientWrapper.REPL_SET_MEMBERS, membersList );
    when( collection.findOne() ).thenReturn( basicDBObject );
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    verify( mockMongoUtilLogger ).info(
        BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined" ) );
    assertThat( tags.size(), equalTo( 0 ) );
  }

  @Test
  public void testGetAllTagsRepSetNull() throws MongoDbException {
    when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    when( mockDB.getCollection( NoAuthMongoClientWrapper.REPL_SET_COLLECTION ) )
        .thenReturn( collection );
    when( collection.findOne() ).thenReturn( null );
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    verify( mockMongoUtilLogger ).info(
        BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined" ) );
    assertThat( tags.size(), equalTo( 0 ) );
  }

  @Test
  public void testGetAllTagsRepSetUnexpectedType() throws MongoDbException {
    when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    when( mockDB.getCollection( NoAuthMongoClientWrapper.REPL_SET_COLLECTION ) )
        .thenReturn( collection );
    DBObject dbObj = mock( DBObject.class );
    when( collection.findOne() ).thenReturn( dbObj );
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    verify( mockMongoUtilLogger ).info(
        BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined" ) );
    assertThat( tags.size(), equalTo( 0 ) );
  }

  @Test
  public void testGetAllTags() throws MongoDbException {
    setupMockedReplSet();
    List<String> tags = noAuthMongoClientWrapper.getAllTags();
    Collections.sort( tags, String.CASE_INSENSITIVE_ORDER );
    assertThat( tags, equalTo( Arrays
            .asList( "\"dc.one\" : \"primary\"",
                "\"dc.three\" : \"slave2\"",
                "\"dc.two\" : \"slave1\"",
                "\"use\" : \"production\"" ) ) );
  }

  @Test
  public void testGetCreateCollection() throws MongoDbException {
    initFakeDb();
    noAuthMongoClientWrapper.getCollection( "fakeDb", "collection" );
    verify( mockDB ).getCollection( "collection" );
    noAuthMongoClientWrapper.createCollection( "fakeDb", "newCollection" );
    verify( mockDB ).createCollection( "newCollection", null );
  }

  @Test
  public void testClientDelegation() {
    noAuthMongoClientWrapper.dispose();
    verify( mockMongoClient ).close();
    noAuthMongoClientWrapper.getReplicaSetStatus();
    verify( mockMongoClient ).getReplicaSetStatus();
  }



  private void setupMockedReplSet() {
    when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    when( mockDB.getCollection( NoAuthMongoClientWrapper.REPL_SET_COLLECTION ) )
        .thenReturn( collection );
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( NoAuthMongoClientWrapper.REPL_SET_MEMBERS );
    DBObject basicDBObject = new BasicDBObject( NoAuthMongoClientWrapper.REPL_SET_MEMBERS, members );
    when( collection.findOne() ).thenReturn( basicDBObject );
  }


}
