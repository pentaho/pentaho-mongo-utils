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
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
import org.junit.Test;
import org.pentaho.mongo.MongoDbException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoNoAuthWrapperTest {
  public static String REP_SET_CONFIG = "{\"_id\" : \"foo\", \"version\" : 1, " + "\"members\" : [" + "{"
    + "\"_id\" : 0, " + "\"host\" : \"palladium.lan:27017\", " + "\"tags\" : {" + "\"dc.one\" : \"primary\", "
    + "\"use\" : \"production\"" + "}" + "}, " + "{" + "\"_id\" : 1, " + "\"host\" : \"palladium.local:27018\", "
    + "\"tags\" : {" + "\"dc.two\" : \"slave1\"" + "}" + "}, " + "{" + "\"_id\" : 2, "
    + "\"host\" : \"palladium.local:27019\", " + "\"tags\" : {" + "\"dc.three\" : \"slave2\", "
    + "\"use\" : \"production\"" + "}" + "}" + "]," + "\"settings\" : {" + "\"getLastErrorModes\" : { "
    + "\"DCThree\" : {" + "\"dc.three\" : 1" + "}" + "}" + "}" + "}";

  private static final String TAG_SET = "{\"use\" : \"production\"}";
  private static final String TAG_SET_LIST = "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"a\" },"
    + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"d\" },"
    + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"mem\": \"r\"}";

  private MongoClient client = null;

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
