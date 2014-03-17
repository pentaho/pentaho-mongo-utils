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

package org.pentaho.mongo;

import com.mongodb.MongoClientOptions;
import org.junit.Test;
import org.mockito.Mockito;

import static junit.framework.Assert.*;
import static org.pentaho.mongo.MongoProp.*;

public class MongoPropertiesTest {
  @Test
  public void testBuildsMongoClientOptions() throws Exception {
    MongoProperties props = new MongoProperties.Builder()
      .set(connectionsPerHost, "127" )
      .set(connectTimeout, "333" )
      .set( maxAutoConnectRetryTime, "3000" )
      .set( maxWaitTime, "12345" )
      .set( alwaysUseMBeans, "true" )
      .set( autoConnectRetry, "true" )
      .set( cursorFinalizerEnabled, "false" )
      .set( socketKeepAlive, "true" )
      .set( socketTimeout, "4" )
      .build();
    MongoUtilLogger log = Mockito.mock( MongoUtilLogger.class );
    MongoClientOptions options = props.buildMongoClientOptions( log );
    assertEquals( 127, options.getConnectionsPerHost() );
    assertEquals( 333, options.getConnectTimeout() );
    assertEquals( 3000, options.getMaxAutoConnectRetryTime() );
    assertEquals( 12345, options.getMaxWaitTime() );
    assertTrue( options.isAlwaysUseMBeans() );
    assertTrue( options.isAutoConnectRetry() );
    assertFalse( options.isCursorFinalizerEnabled() );
    assertTrue( options.isSocketKeepAlive() );
    assertEquals( 4, options.getSocketTimeout() );
  }

  @Test
  public void testBuildsMongoClientOptionsDefaults() throws Exception {
    MongoProperties props = new MongoProperties.Builder().build();
    MongoUtilLogger log = Mockito.mock( MongoUtilLogger.class );
    MongoClientOptions options = props.buildMongoClientOptions( log );
    assertEquals( 100, options.getConnectionsPerHost() );
    assertEquals( 10000, options.getConnectTimeout() );
    assertEquals( 1000, options.getMaxAutoConnectRetryTime() );
    assertEquals( 120000, options.getMaxWaitTime() );
    assertFalse( options.isAlwaysUseMBeans() );
    assertFalse( options.isAutoConnectRetry() );
    assertTrue( options.isCursorFinalizerEnabled() );
    assertFalse( options.isSocketKeepAlive() );
    assertEquals( 0, options.getSocketTimeout() );
  }
}
