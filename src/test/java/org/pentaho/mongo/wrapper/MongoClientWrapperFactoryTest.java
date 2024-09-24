/*!
 * Copyright 2010 - 2021 Hitachi Vantara.  All rights reserved.
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
