/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.mongo.wrapper;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;
import static org.junit.Assert.assertThat;


public class MongoClientWrapperFactoryTest {

  @Mock DefaultMongoClientFactory mongoClientFactory;
  @Mock MongoUtilLogger logger;

  @Before
  public void before() {
    MockitoAnnotations.initMocks( this );
    NoAuthMongoClientWrapper.clientFactory = mongoClientFactory;
  }

  @Test
  public void testCreateMongoClientWrapper() throws Exception {
    MongoClientWrapper wrapper = MongoClientWrapperFactory
        .createMongoClientWrapper(
        new MongoProperties.Builder()
            .set( MongoProp.USERNAME, "user" )
            .set( MongoProp.PASSWORD, "password" )
            .set( MongoProp.DBNAME, "dbname" ).build(),
        logger );
    assertThat( wrapper, CoreMatchers.instanceOf( UsernamePasswordMongoClientWrapper.class ) );

    wrapper = MongoClientWrapperFactory
        .createMongoClientWrapper(
            new MongoProperties.Builder()
                .set( MongoProp.USE_KERBEROS, "false" )
                .build(),
            logger );
    assertThat( wrapper, CoreMatchers.instanceOf( NoAuthMongoClientWrapper.class ) );
  }

  @Test
  public void testExpCreateConnectionStringMongoClientWrapper() throws Exception {
    try {
      MongoClientWrapper wrapper = MongoClientWrapperFactory
              .createConnectionStringMongoClientWrapper( "http://mongoDb1:", logger );
      //Assert.fail( "expected exception" );
    } catch ( Exception mde ) {
      Assert.assertThat( mde, CoreMatchers.instanceOf( IllegalArgumentException.class ) );
    }
  }

  @Test
  public void testCreateConnectionStringMongoClientWrapper() throws Exception {
    MongoClientWrapper wrapper = MongoClientWrapperFactory
            .createConnectionStringMongoClientWrapper( "mongodb://localhost:27017", logger );
    assertThat( wrapper, CoreMatchers.instanceOf( ConnectionStringMongoClientWrapper.class ) );
  }
}
