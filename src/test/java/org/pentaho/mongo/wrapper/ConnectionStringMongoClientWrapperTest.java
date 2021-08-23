/*!
  * Copyright 2021 Hitachi Vantara.  All rights reserved.
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

import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.matchers.StringContains;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.pentaho.mongo.BaseMessages;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoUtilLogger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class ConnectionStringMongoClientWrapperTest {

  public static String REP_SET_CONFIG = "{\"_id\" : \"foo\", \"version\" : 1, " + "\"members\" : [" + "{"
          + "\"_id\" : 0, " + "\"host\" : \"palladium.lan:27017\", " + "\"tags\" : {" + "\"dc.one\" : \"primary\", "
          + "\"use\" : \"production\"" + "}" + "}, " + "{" + "\"_id\" : 1, " + "\"host\" : \"palladium.local:27018\", "
          + "\"tags\" : {" + "\"dc.two\" : \"slave1\"" + "}" + "}, " + "{" + "\"_id\" : 2, "
          + "\"host\" : \"palladium.local:27019\", " + "\"tags\" : {" + "\"dc.three\" : \"slave2\", "
          + "\"use\" : \"production\"" + "}" + "}" + "]," + "\"settings\" : {" + "\"getLastErrorModes\" : { "
          + "\"DCThree\" : {" + "\"dc.three\" : 1" + "}" + "}" + "}" + "}";
  private static final String TAG_SET = "{\"use\" : \"production\"}";
  private static final String TAG_SET2 = "{\"use\" : \"ops\"}";
  private static Class<?> PKG = ConnectionStringMongoClientWrapper.class;
  @Mock private DefaultMongoClientFactory mongoClientFactory;
  @Mock private MongoClient mockMongoClient;
  @Mock private MongoUtilLogger mockMongoUtilLogger;
  @Mock private DB mockDB;
  @Mock DBCollection collection;
  @Mock private RuntimeException runtimeException;
  private ConnectionStringMongoClientWrapper connectionStringMongoClientWrapper;
  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks( this );
    Mockito.when( mongoClientFactory.getConnectionStringMongoClient( Mockito.any( String.class ) ) )
              .thenReturn( mockMongoClient );
    ConnectionStringMongoClientWrapper.clientFactory = mongoClientFactory;
    connectionStringMongoClientWrapper = new ConnectionStringMongoClientWrapper( Mockito.anyString(), mockMongoUtilLogger );
  }

  @Test
  public void testPerform() throws Exception {
    MongoDBAction mockMongoDBAction = Mockito.mock( MongoDBAction.class );
    connectionStringMongoClientWrapper.perform( "Test", mockMongoDBAction );
    Mockito.verify( mockMongoDBAction, Mockito.times( 1 ) ).perform( connectionStringMongoClientWrapper.getDb( "Test" ) );
  }

  @Test
  public void testGetLastErrorMode() throws MongoDbException {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    DBCollection dbCollection = Mockito.mock( DBCollection.class );
    Mockito.when( dbCollection.findOne() ).thenReturn( config );
    Mockito.when( mockMongoClient.getDB( connectionStringMongoClientWrapper.LOCAL_DB ) )
        .thenReturn( mockDB );
    Mockito.when( mockDB.getCollection( connectionStringMongoClientWrapper.REPL_SET_COLLECTION ) )
         .thenReturn( dbCollection );
    Assert.assertThat( connectionStringMongoClientWrapper.getLastErrorModes(), IsEqual.equalTo( Arrays.asList( "DCThree" ) ) );
  }
  @Test
  public void testGetAllReplicaSetMemberRecords() {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( connectionStringMongoClientWrapper.REPL_SET_MEMBERS );
    assertNotNull( members );
    Assert.assertTrue( members instanceof BasicDBList );
    Assert.assertEquals( 3, ( (BasicDBList) members ).size() );
  }

  @Test
  public void testSetupAllTags() {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( connectionStringMongoClientWrapper.REPL_SET_MEMBERS );
    List<String> allTags = connectionStringMongoClientWrapper.setupAllTags( (BasicDBList) members );
    Assert.assertEquals( 4, allTags.size() );
  }

  @Test
  public void testGetReplicaSetMembersBadInput() throws MongoDbException {
    setupMockedReplSet();
    try {
      connectionStringMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets( null );
      Assert.fail( "expected exception" );
    } catch ( Exception e ) {
      Assert.assertThat( e, CoreMatchers.instanceOf( MongoDbException.class ) );
    }
  }
  @Test
  public void testGetReplicaSetMembersDoesntSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();
    List<DBObject> tagSets = new ArrayList<DBObject>(); // tags to satisfy
    DBObject tSet = (DBObject) JSON.parse( TAG_SET2 );
    tagSets.add( tSet );
    List<String> satisfy =
       connectionStringMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets( tagSets );
    // no replica set members have the "use : ops" tag in their tag sets
    Assert.assertEquals( 0, satisfy.size() );
  }
  @Test
  public void testGetReplicaSetMembersThatSatisfyTagSetsThrowsOnDbError() throws MongoDbException {
    setupMockedReplSet();
    List<DBObject> tagSets = new ArrayList<DBObject>(); // tags to satisfy
    DBObject tSet = (DBObject) JSON.parse( TAG_SET );
    tagSets.add( tSet );
    Mockito.doThrow( runtimeException ).when( mockMongoClient )
            .getDB( connectionStringMongoClientWrapper.LOCAL_DB );
    try {
      connectionStringMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets( tagSets );
      Assert.fail( "expected exception." );
    } catch ( Exception e ) {
      Assert.assertThat( e, CoreMatchers.instanceOf( MongoDbException.class ) );
    }
  }
  @Test
  public void operationsDelegateToMongoClient() throws MongoDbException {
    connectionStringMongoClientWrapper.getDatabaseNames();
    Mockito.verify( mockMongoClient ).getDatabaseNames();

    connectionStringMongoClientWrapper.getDb( "foo" );
    Mockito.verify( mockMongoClient ).getDB( "foo" );

    Mockito.when( mockMongoClient.getDB( "foo" ) ).thenReturn( mockDB );
    connectionStringMongoClientWrapper.getCollectionsNames( "foo" );
    Mockito.verify( mockDB ).getCollectionNames();
  }
  @Test
  public void testGetIndex() throws MongoDbException {
    Mockito.when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    Mockito.when( mockDB.collectionExists( "collection" ) ).thenReturn( true );
    DBCollection collection = Mockito.mock( DBCollection.class );
    DBObject indexInfoObj = Mockito.mock( DBObject.class );
    Mockito.when( indexInfoObj.toString() ).thenReturn( "indexInfo" );
    List<DBObject> indexInfo = Arrays.asList( indexInfoObj );
    Mockito.when( collection.getIndexInfo() ).thenReturn( indexInfo );
    Mockito.when( mockDB.getCollection( "collection" ) ).thenReturn( collection );

    Assert.assertThat( connectionStringMongoClientWrapper.getIndexInfo( "fakeDb", "collection" ),
       IsEqual.equalTo( Arrays.asList( "indexInfo" ) ) );
  }
  @Test
  public void testGetAllTagsNoDB() throws MongoDbException {
    List<String> tags = connectionStringMongoClientWrapper.getAllTags();
    Mockito.verify( mockMongoUtilLogger ).info(
      BaseMessages.getString( PKG, "MongoConnectionStringWrapper.Message.Warning.LocalDBNotAvailable" ) );
    Assert.assertThat( tags.size(), IsEqual.equalTo( 0 ) );
  }

  @Test
  public void testGetAllTagsNoRepSet() throws MongoDbException {
    Mockito.when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    List<String> tags = connectionStringMongoClientWrapper.getAllTags();
    Mockito.verify( mockMongoUtilLogger ).info(
      BaseMessages.getString( PKG,
      "MongoConnectionStringWrapper.Message.Warning.ReplicaSetCollectionUnavailable" ) );
    Assert.assertThat( tags.size(), IsEqual.equalTo( 0 ) );
  }

  @Test
  public void testGetAllTagsRepSetEmtpy() throws MongoDbException {
    Mockito.when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    Mockito.when( mockDB.getCollection( NoAuthMongoClientWrapper.REPL_SET_COLLECTION ) )
            .thenReturn( collection );
    DBObject membersList = new BasicDBList();
    DBObject basicDBObject = new BasicDBObject( NoAuthMongoClientWrapper.REPL_SET_MEMBERS, membersList );
    Mockito.when( collection.findOne() ).thenReturn( basicDBObject );
    List<String> tags = connectionStringMongoClientWrapper.getAllTags();
    Mockito.verify( mockMongoUtilLogger ).info(
            BaseMessages.getString( PKG,
                    "MongoConnectionStringWrapper.Message.Warning.NoReplicaSetMembersDefined" ) );
    Assert.assertThat( tags.size(), IsEqual.equalTo( 0 ) );
  }

  @Test
  public void testGetAllTagsRepSetNull() throws MongoDbException {
    Mockito.when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    Mockito.when( mockDB.getCollection( NoAuthMongoClientWrapper.REPL_SET_COLLECTION ) )
            .thenReturn( collection );
    Mockito.when( collection.findOne() ).thenReturn( null );
    List<String> tags = connectionStringMongoClientWrapper.getAllTags();
    Mockito.verify( mockMongoUtilLogger ).info(
            BaseMessages.getString( PKG,
                    "MongoConnectionStringWrapper.Message.Warning.NoReplicaSetMembersDefined" ) );
    Assert.assertThat( tags.size(), IsEqual.equalTo( 0 ) );
  }

  @Test
  public void testGetAllTagsRepSetUnexpectedType() throws MongoDbException {
    Mockito.when( mockMongoClient.getDB( NoAuthMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    Mockito.when( mockDB.getCollection( NoAuthMongoClientWrapper.REPL_SET_COLLECTION ) )
            .thenReturn( collection );
    DBObject dbObj = Mockito.mock( DBObject.class );
    Mockito.when( collection.findOne() ).thenReturn( dbObj );
    List<String> tags = connectionStringMongoClientWrapper.getAllTags();
    Mockito.verify( mockMongoUtilLogger ).info(
            BaseMessages.getString( PKG,
                    "MongoConnectionStringWrapper.Message.Warning.NoReplicaSetMembersDefined" ) );
    Assert.assertThat( tags.size(), IsEqual.equalTo( 0 ) );
  }

  @Test
  public void testGetAllTags() throws MongoDbException {
    setupMockedReplSet();
    List<String> tags = connectionStringMongoClientWrapper.getAllTags();
    Collections.sort( tags, String.CASE_INSENSITIVE_ORDER );
    Assert.assertThat( tags, IsEqual.equalTo( Arrays
            .asList( "\"dc.one\" : \"primary\"",
                    "\"dc.three\" : \"slave2\"",
                    "\"dc.two\" : \"slave1\"",
                    "\"use\" : \"production\"" ) ) );
  }

  @Test
  public void testGetReplicaSetMembersThatSatisfyTagSets() throws MongoDbException {
    setupMockedReplSet();

    List<DBObject> tagSets = new ArrayList<DBObject>(); // tags to satisfy

    DBObject tSet = (DBObject) JSON.parse( TAG_SET );
    tagSets.add( tSet );

    List<String> satisfy =
            connectionStringMongoClientWrapper.getReplicaSetMembersThatSatisfyTagSets( tagSets );
    // two replica set members have the "use : production" tag in their tag sets
    Assert.assertEquals( 2, satisfy.size() );
    Assert.assertThat( satisfy.get( 0 ), StringContains.containsString( "palladium.lan:27017" ) );
    Assert.assertThat( satisfy.get( 1 ), StringContains.containsString( "palladium.local:27019" ) );
  }

  @Test
  public void mongoExceptionsPropogate() {
    Mockito.doThrow( runtimeException ).when( mockMongoClient ).getDatabaseNames();
    try {
      connectionStringMongoClientWrapper.getDatabaseNames();
      Assert.fail( "expected exception" );
    } catch ( Exception mde ) {
      Assert.assertThat( mde, CoreMatchers.instanceOf( MongoDbException.class ) );
    }
    Mockito.doThrow( runtimeException ).when( mockMongoClient ).getDB( "foo" );
    try {
      connectionStringMongoClientWrapper.getDb( "foo" );
      Assert.fail( "expected exception" );
    } catch ( Exception mde ) {
      Assert.assertThat( mde, CoreMatchers.instanceOf( MongoDbException.class ) );
    }
  }

  @Test
  public void mongoGetCollNamesExceptionPropgates() {
    Mockito.when( mockMongoClient.getDB( "foo" ) ).thenReturn( mockDB );
    Mockito.doThrow( runtimeException ).when( mockDB ).getCollectionNames();
    try {
      connectionStringMongoClientWrapper.getCollectionsNames( "foo" );
      Assert.fail( "expected exception" );
    } catch ( Exception mde ) {
      Assert.assertThat( mde, CoreMatchers.instanceOf( MongoDbException.class ) );
    }
  }

  @Test
  public void testGetIndexCollectionDoesntExist() throws MongoDbException {
    Mockito.when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    Mockito.when( mockDB.collectionExists( "collection" ) ).thenReturn( false );
    DBCollection collection = Mockito.mock( DBCollection.class );
    DBObject indexInfoObj = Mockito.mock( DBObject.class );
    Mockito.when( indexInfoObj.toString() ).thenReturn( "indexInfo" );
    List<DBObject> indexInfo = Arrays.asList( indexInfoObj );
    Mockito.when( collection.getIndexInfo() ).thenReturn( indexInfo );
    Mockito.when( mockDB.getCollection( "collection" ) ).thenReturn( collection );
    Assert.assertThat( connectionStringMongoClientWrapper.getIndexInfo( "fakeDb", "collection" ),
            IsEqual.equalTo( Arrays.asList( "indexInfo" ) ) );
    Mockito.verify( mockDB ).createCollection( "collection", null );
  }

  @Test
  public void testGetIndexCollectionNotSpecified() throws MongoDbException {
    Mockito.when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    try {
      connectionStringMongoClientWrapper.getIndexInfo( "fakeDb", "" );
      Assert.fail( "expected exception" );
    } catch ( Exception e ) {
      Assert.assertThat( e, CoreMatchers.instanceOf( MongoDbException.class ) );
    }
  }

  @Test
  public void getIndexInfoErrorConditions() {
    try {
      connectionStringMongoClientWrapper.getIndexInfo( "fakeDb", "collection" );
      Assert.fail( "expected exception since DB is null" );
    } catch ( Exception e ) {
    }
    try {
      connectionStringMongoClientWrapper.getIndexInfo( "fakeDb", "" );
      Assert.fail( "expected exception since no collection specified." );
    } catch ( Exception e ) {
    }
    Mockito.when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    Mockito.when( mockDB.collectionExists( "collection" ) ).thenReturn( true );
    try {
      connectionStringMongoClientWrapper.getIndexInfo( "fakeDb", "collection" );
      Assert.fail( "expected exception since null collection" );
    } catch ( Exception e ) {
      Mockito.verify( mockDB ).getCollection( "collection" );
      Assert.assertThat( e.getMessage(),
        StringContains.containsString( BaseMessages.getString( PKG,
               "MongoConnectionStringWrapper.ErrorMessage.UnableToGetInfoForCollection",
              "collection" ) ) );
    }
  }

  @Test
  public void testGetIndexNoIndexThrows() {
    initFakeDb();
    try {
      connectionStringMongoClientWrapper.getIndexInfo( "fakeDb", "collection" );
      Assert.fail( "expected exception since no index info" );
    } catch ( Exception e ) {
      Mockito.verify( mockDB ).getCollection( "collection" );
      Assert.assertThat( e.getMessage(),
        StringContains.containsString( BaseMessages.getString( PKG,
             "MongoConnectionStringWrapper.ErrorMessage.UnableToGetInfoForCollection",
             "collection" ) ) );
    }
  }

  private void initFakeDb() {
    Mockito.when( mockMongoClient.getDB( "fakeDb" ) ).thenReturn( mockDB );
    Mockito.when( mockDB.collectionExists( "collection" ) ).thenReturn( true );
    Mockito.when( mockDB.getCollection( "collection" ) ).thenReturn( collection );
  }

  @Test
  public void testGetCreateCollection() throws MongoDbException {
    initFakeDb();
    connectionStringMongoClientWrapper.getCollection( "fakeDb", "collection" );
    Mockito.verify( mockDB ).getCollection( "collection" );
    connectionStringMongoClientWrapper.createCollection( "fakeDb", "newCollection" );
    Mockito.verify( mockDB ).createCollection( "newCollection", null );
  }
  @Test
  public void testClientDelegation() throws MongoDbException {
    connectionStringMongoClientWrapper.dispose();
    Mockito.verify( mockMongoClient ).close();
    connectionStringMongoClientWrapper.getReplicaSetStatus();
    Mockito.verify( mockMongoClient ).getReplicaSetStatus();
  }
  private void setupMockedReplSet() {
    Mockito.when( mockMongoClient.getDB( connectionStringMongoClientWrapper.LOCAL_DB ) ).thenReturn( mockDB );
    Mockito.when( mockDB.getCollection( connectionStringMongoClientWrapper.REPL_SET_COLLECTION ) )
         .thenReturn( collection );
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( connectionStringMongoClientWrapper.REPL_SET_MEMBERS );
    DBObject basicDBObject = new BasicDBObject( connectionStringMongoClientWrapper.REPL_SET_MEMBERS, members );
    Mockito.when( collection.findOne() ).thenReturn( basicDBObject );
  }
}
