/*!
* Copyright 2010 - 2015 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.mongo;

import com.mongodb.ReadPreference;
import com.mongodb.TaggableReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static junit.framework.Assert.fail;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MongoPropToOptionTest {
  @Mock private MongoUtilLogger log;

  @Before
  public void before() {
    MockitoAnnotations.initMocks( this );
  }


  private static final String TAG_SET = "{\"use\" : \"production\"}";

  private static final String TAG_SET_LIST = "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"a\" },"
      + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"d\" },"
      + "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"mem\": \"r\"}";

  @Test
  public void testConfigureReadPref() throws MongoDbException {
    MongoProperties.Builder builder = new MongoProperties.Builder()
        .set( MongoProp.tagSet, TAG_SET );
    MongoUtilLogger logger = mock( MongoUtilLogger.class );
    MongoPropToOption propToOption = new MongoPropToOption( logger );

    // Verify PRIMARY with tag sets causes a warning
    ReadPreference readPreference = propToOption.readPrefValue( builder.build() );
    // should warn.  tag sets with a PRIMARY read pref are invalid.
    verify( logger ).warn( BaseMessages.getString(
        this.getClass(), "MongoPropToOption.Message.Warning.PrimaryReadPrefWithTagSets" ), null  );
    // defaults to primary if unset
    assertEquals( ReadPreference.primary(), readPreference );

    String[] tagSetTests = new String[] { TAG_SET, TAG_SET_LIST, null };
    for ( String tagSet : tagSetTests ) {
      for ( String readPreferenceType : NamedReadPreference.getPreferenceNames() ) {
        builder.set( MongoProp.readPreference, readPreferenceType );
        builder.set( MongoProp.tagSet, tagSet );
        testReadPrefScenario( builder.build(), propToOption );
      }
    }
    // Test invalid READ_PREFERENCE
    builder.set( MongoProp.readPreference, "Invalid" );
    try {
      propToOption.readPrefValue( builder.build() );
      fail( "Expected an exception due to invalid read preference." );
    } catch ( MongoDbException e ) {
      assertEquals( "The specified READ_PREFERENCE is invalid:  {0}.  "
              + "Should be one of:  [primary, primaryPreferred, secondary, secondaryPreferred, nearest].",
          e.getMessage() );
    }
  }

  private void testReadPrefScenario( MongoProperties props, MongoPropToOption propToOption ) throws MongoDbException {
    ReadPreference readPreference = propToOption.readPrefValue( props );

    assertEquals( props.get( MongoProp.readPreference ), readPreference.getName() );

    String tagSet = props.get( MongoProp.tagSet );
    if ( tagSet == null ) {
      tagSet = "";
    }
    if ( "primary".equals( props.get( MongoProp.readPreference ) ) ) {
      // no tag sets
      assertFalse( readPreference instanceof TaggableReadPreference );
    } else {
      assertTrue( readPreference instanceof TaggableReadPreference );
      assertEquals( JSON.parse( "[" + tagSet + "]" ),
          ( (TaggableReadPreference) readPreference ).getTagSets() );
    }
  }

  @Test
  public void testGetTagSets() throws MongoDbException {
    MongoProperties.Builder builder = new MongoProperties.Builder();
    builder.set( MongoProp.tagSet, TAG_SET );
    MongoUtilLogger logger = mock( MongoUtilLogger.class );

    MongoPropToOption wrapper = new MongoPropToOption( logger );
    assertEquals( JSON.parse( TAG_SET ), wrapper.getTagSets( builder.build() )[ 0 ] );
    assertEquals( 1, wrapper.getTagSets( builder.build() ).length );

    String tagSet2 = "{ \"disk\": \"ssd\", \"use\": \"reporting\" }";
    builder.set( MongoProp.tagSet, tagSet2 );
    assertEquals( JSON.parse( tagSet2 ), wrapper.getTagSets( builder.build() )[ 0 ] );
    assertEquals( 1, wrapper.getTagSets( builder.build() ).length );



    builder.set( MongoProp.tagSet, TAG_SET_LIST );
    assertEquals( JSON.parse( "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"a\" }" ),
        wrapper.getTagSets( builder.build() )[ 0 ] );
    assertEquals( 3, wrapper.getTagSets( builder.build() ).length );

    String tagsAsArray = "[" + TAG_SET_LIST + "]";
    builder.set( MongoProp.tagSet, tagsAsArray );
    assertEquals( JSON.parse( "{ \"disk\": \"ssd\", \"use\": \"reporting\", \"rack\": \"a\" }" ),
        wrapper.getTagSets( builder.build() )[ 0 ] );
    assertEquals( 3, wrapper.getTagSets( builder.build() ).length );

    // extra curly paren.
    String tagSetInvalid = " { key : 'value', key2 : 'value2'}, { key : 'value3' } } ";
    builder.set( MongoProp.tagSet, tagSetInvalid );
    try {
      wrapper.getTagSets( builder.build() );
      fail( "Expected a parse exception" );
    } catch ( MongoDbException e ) {
      assertEquals( "The tagSet property specified cannot be parsed:  "
              + "[ { key : 'value', key2 : 'value2'}, { key : 'value3' } } ]",
          e.getMessage() );
      assertTrue( e.getCause() instanceof JSONParseException );
    }
  }

  @Test
  public void testGetVals() {
    assertThat( new MongoPropToOption( log ).intValue( "invalid", 123 ), equalTo( 123 ) );
    assertThat( new MongoPropToOption( log ).intValue( "234", 123 ), equalTo( 234 ) );
    assertThat( new MongoPropToOption( log ).longValue( "invalid", 123l ), equalTo( 123l ) );
    assertThat( new MongoPropToOption( log ).longValue( "", 123l ), equalTo( 123l ) );
    assertThat( new MongoPropToOption( log ).longValue( "234", 123l ), equalTo( 234l ) );
    assertThat( new MongoPropToOption( log ).boolValue( "", true ), equalTo( true ) );
    assertThat( new MongoPropToOption( log ).boolValue( "false", true ), equalTo( false ) );
  }

  @Test
  public void testWriteConcernDefaultValue() throws MongoDbException {
    MongoProperties props = new MongoProperties.Builder().set( MongoProp.writeConcern, "1" ).build();
    MongoPropToOption mongoPropToOption = new MongoPropToOption( log );
    WriteConcern writeConcern = mongoPropToOption.writeConcernValue( props );
    assertThat( (Integer) writeConcern.getWObject(), equalTo( 1 ) );
  }

  @Test
  public void testWriteConcernWithWriteTimeout() throws MongoDbException {
    MongoProperties props = new MongoProperties.Builder()
        .set( MongoProp.writeConcern, "2" )
        .set( MongoProp.wTimeout, "1010" )
        .set( MongoProp.JOURNALED, "false" )
        .build();
    MongoPropToOption mongoPropToOption = new MongoPropToOption( log );
    WriteConcern writeConcern = mongoPropToOption.writeConcernValue( props );
    assertThat( (Integer) writeConcern.getWObject(), equalTo( 2 ) );
    assertThat( writeConcern.getWtimeout(), equalTo( 1010 ) );
  }

  @Test
  public void testWriteConcernWithInvalidWriteTimeout() throws MongoDbException {
    MongoProperties props = new MongoProperties.Builder()
        .set( MongoProp.writeConcern, "2" )
        .set( MongoProp.wTimeout, "FooBar" )
        .set( MongoProp.JOURNALED, "false" )
        .build();
    MongoPropToOption mongoPropToOption = new MongoPropToOption( log );
    try {
      mongoPropToOption.writeConcernValue( props );
    } catch ( MongoDbException e ) {
      assertThat( e.getCause(), instanceOf( NumberFormatException.class ) );
      return;
    }
    fail( "expected MongoDbException" );
  }

  @Test
  public void testNonNumericWriteConcernValue() throws MongoDbException {
    MongoProperties props = new MongoProperties.Builder()
        .set( MongoProp.writeConcern, "majority" )
        .set( MongoProp.wTimeout, "1010" )
        .set( MongoProp.JOURNALED, "false" )
        .build();
    MongoPropToOption mongoPropToOption = new MongoPropToOption( log );
    WriteConcern wc = mongoPropToOption.writeConcernValue( props );
    assertThat( wc.getWString(), equalTo( "majority" ) );
  }

  @Test
  public void getReadPrefUnset() throws MongoDbException {
    MongoPropToOption mongoPropToOption = new MongoPropToOption( null );
    assertThat( mongoPropToOption.readPrefValue(
            new MongoProperties.Builder()
                .set( MongoProp.readPreference, "" )
                .build() ),
        equalTo( null ) );
  }

}
