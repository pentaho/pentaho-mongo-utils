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

package org.pentaho.mongo;

import com.mongodb.MongoClientOptions;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLSocketFactory;

import static junit.framework.Assert.*;
import static org.pentaho.mongo.MongoProp.*;

public class MongoPropertiesTest {
  @Test
  public void testBuildsMongoClientOptions() throws Exception {
    MongoProperties props = new MongoProperties.Builder()
      .set( connectionsPerHost, "127" )
      .set( connectTimeout, "333" )
      .set( maxWaitTime, "12345" )
      .set( cursorFinalizerEnabled, "false" )
      .set( socketKeepAlive, "true" )
      .set( socketTimeout, "4" )
      .set( useSSL, "true" )
      .build();
    MongoUtilLogger log = Mockito.mock( MongoUtilLogger.class );
    MongoClientOptions options = props.buildMongoClientOptions( log );
    assertEquals( 127, options.getConnectionsPerHost() );
    assertEquals( 333, options.getConnectTimeout() );
    assertEquals( 12345, options.getMaxWaitTime() );
    assertFalse( options.isCursorFinalizerEnabled() );
    assertTrue( options.isSocketKeepAlive() );
    assertEquals( 4, options.getSocketTimeout() );
    assertTrue( options.getSocketFactory() instanceof SSLSocketFactory );
  }

  @Test
  public void testBuildsMongoClientOptionsDefaults() throws Exception {
    MongoProperties props = new MongoProperties.Builder().build();
    MongoUtilLogger log = Mockito.mock( MongoUtilLogger.class );
    MongoClientOptions options = props.buildMongoClientOptions( log );
    assertEquals( 100, options.getConnectionsPerHost() );
    assertEquals( 10000, options.getConnectTimeout() );
    assertEquals( 120000, options.getMaxWaitTime() );
    assertTrue( options.isCursorFinalizerEnabled() );
    assertFalse( options.isSocketKeepAlive() );
    assertEquals( 0, options.getSocketTimeout() );
    assertFalse( options.getSocketFactory() instanceof SSLSocketFactory );
  }
}
