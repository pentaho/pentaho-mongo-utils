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
