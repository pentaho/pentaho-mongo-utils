/*!
 * Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
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

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.junit.Test;
import org.pentaho.mongo.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MongoNoAuthWrapperTest {
  protected static String s_testData = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
    + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
    + "\"name\" : \"george\", \"aNumber\" : 42 }";
  protected static String s_testData2 = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
    + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
    + "\"name\" : \"george\", \"aNumber\" : \"Forty two\" }";
  protected static String s_testData3 = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
    + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
    + "\"name\" : \"george\", \"aNumber\" : 42, anotherNumber : 32, numberArray : [1,2,3] }";
  protected static String s_testData4 = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
    + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
    + "\"name\" : \"george\", \"aNumber\" : \"Forty two\", anotherNumber : null, numberArray : [null,2,3] }";
  public static String REP_SET_CONFIG = "{\"_id\" : \"foo\", \"version\" : 1, " + "\"members\" : [" + "{"
    + "\"_id\" : 0, " + "\"host\" : \"palladium.lan:27017\", " + "\"tags\" : {" + "\"dc.one\" : \"primary\", "
    + "\"use\" : \"production\"" + "}" + "}, " + "{" + "\"_id\" : 1, " + "\"host\" : \"palladium.local:27018\", "
    + "\"tags\" : {" + "\"dc.two\" : \"slave1\"" + "}" + "}, " + "{" + "\"_id\" : 2, "
    + "\"host\" : \"palladium.local:27019\", " + "\"tags\" : {" + "\"dc.three\" : \"slave2\", "
    + "\"use\" : \"production\"" + "}" + "}" + "]," + "\"settings\" : {" + "\"getLastErrorModes\" : { "
    + "\"DCThree\" : {" + "\"dc.three\" : 1" + "}" + "}" + "}" + "}";

  public static String TAG_SET = "{\"use\" : \"production\"}";

  private MongoClient client = null;


  @Test
  public void testGetTagSets() throws MongoDbException {
    MongoProperties props = new MongoProperties();
    props.set( MongoProp.TAG_SET, TAG_SET );
    MongoClient client = mock( MongoClient.class );

    NoAuthMongoClientWrapper wrapper = new NoAuthMongoClientWrapper( client, null );
    assertEquals( JSON.parse( TAG_SET ), wrapper.getTagSets( props )[ 0 ] );
    assertEquals( 1, wrapper.getTagSets( props ).length );

    String tagSet2 = "{ \"disk\": \"ssd\", \"use\": \"reporting\" }";
    props.set( MongoProp.TAG_SET, tagSet2 );
    assertEquals( JSON.parse( tagSet2 ), wrapper.getTagSets( props )[ 0 ] );
    assertEquals( 1, wrapper.getTagSets( props ).length );

    String tagSet3 = "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"a\" },"
      + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"d\" },"
      + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"mem\": \"r\"}";

    props.set( MongoProp.TAG_SET, tagSet3 );
    assertEquals( JSON.parse( "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"a\" }" ),
      wrapper.getTagSets( props )[ 0 ] );
    assertEquals( 3, wrapper.getTagSets( props ).length );
  }

  @Test
  public void testConfigureReadPref() {
    MongoProperties props = new MongoProperties();
    props.set( MongoProp.TAG_SET, TAG_SET );
    MongoClient client = mock( MongoClient.class );
    MongoClientOptions.Builder builder = MongoClientOptions.builder();
    MongoUtilLogger logger = mock( MongoUtilLogger.class );
    NoAuthMongoClientWrapper wrapper = new NoAuthMongoClientWrapper( client, logger );


    // Verify PRIMARY with tag sets causes a warning
    wrapper.configureReadPref( builder, props );
    // should warn.  tag sets with a PRIMARY read pref are invalid.
    verify( logger ).warn( BaseMessages.getString(
      this.getClass(), "MongoNoAuthWrapper.Message.Warning.PrimaryReadPrefWithTagSets" ), null  );
    // defaults to primary if unset
    assertEquals( ReadPreference.primary(), builder.build().getReadPreference() );

    // Verify options set correctly with SECONDARY and READ_PREFERENCE
    builder = MongoClientOptions.builder();
    props.set( MongoProp.READ_PREFERENCE, "SECONDARY" );
    wrapper.configureReadPref( builder, props );
    MongoClientOptions options = builder.build();
    options.getReadPreference();
    assertEquals( "secondary", options.getReadPreference().getName() );
    assertTrue( options.getReadPreference() instanceof TaggableReadPreference );
    assertEquals( JSON.parse( "[" + TAG_SET + "]" ),
      ( (TaggableReadPreference) options.getReadPreference() ).getTagSets() );
  }

  @Test
  public void testExtractLastErrorMode() throws MongoDbException {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );

    assertTrue( config != null );
    List<String> lastErrorModes = new ArrayList<String>();

    new NoAuthMongoClientWrapper( client, null ).extractLastErrorModes( config, lastErrorModes );

    assertTrue( lastErrorModes.size() == 1 );
    assertEquals( "DCThree", lastErrorModes.get( 0 ) );
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
  public void testGetAllTags() {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( NoAuthMongoClientWrapper.REPL_SET_MEMBERS );

    List<String> allTags = new NoAuthMongoClientWrapper( client, null ).setupAllTags( (BasicDBList) members );

    assertEquals( 4, allTags.size() );
  }

  @Test
  public void testGetReplicaSetMembersThatSatisfyTagSets() {
    List<DBObject> tagSets = new ArrayList<DBObject>(); // tags to satisfy

    DBObject tSet = (DBObject) JSON.parse( TAG_SET );
    tagSets.add( tSet );

    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( NoAuthMongoClientWrapper.REPL_SET_MEMBERS );
    List<DBObject> satisfy =
      new NoAuthMongoClientWrapper( client, null ).checkForReplicaSetMembersThatSatisfyTagSets( tagSets,
        (BasicDBList) members );

    // two replica set members have the "use : production" tag in their tag sets
    assertEquals( 2, satisfy.size() );
  }



}
