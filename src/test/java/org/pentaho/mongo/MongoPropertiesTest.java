/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.mongo;

import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLSocketFactory;

import static org.junit.Assert.*;

public class MongoPropertiesTest {
  @Test
  public void testBuildsMongoClientOptions() throws Exception {
    MongoProperties props = new MongoProperties.Builder()
        .set( MongoProp.connectionsPerHost, "127" )
        .set( MongoProp.connectTimeout, "333" )
        .set( MongoProp.maxWaitTime, "12345" )
        .set( MongoProp.cursorFinalizerEnabled, "false" )
        .set( MongoProp.socketKeepAlive, "true" )
        .set( MongoProp.socketTimeout, "4" )
        .set( MongoProp.useSSL, "true" )
        .set( MongoProp.readPreference, "primary" )
        .set( MongoProp.USE_KERBEROS, "false" )
        .set( MongoProp.USE_ALL_REPLICA_SET_MEMBERS, "false" )
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
    assertEquals( options.getReadPreference(), ReadPreference.primary() );
    assertEquals( props.getReadPreference(), ReadPreference.primary() );
    assertFalse( props.useAllReplicaSetMembers() );
    assertFalse( props.useKerberos() );
    assertEquals( "MongoProperties:\n"
        + "connectionsPerHost=127\n"
        + "connectTimeout=333\n"
        + "cursorFinalizerEnabled=false\n"
        + "HOST=localhost\n"
        + "maxWaitTime=12345\n"
        + "PASSWORD=\n"
        + "readPreference=primary\n"
        + "socketKeepAlive=true\n"
        + "socketTimeout=4\n"
        + "USE_ALL_REPLICA_SET_MEMBERS=false\n"
        + "USE_KERBEROS=false\n"
        + "useSSL=true\n", props.toString() );
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
