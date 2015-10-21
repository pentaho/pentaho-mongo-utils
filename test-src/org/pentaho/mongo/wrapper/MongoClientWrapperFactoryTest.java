package org.pentaho.mongo.wrapper;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

public class MongoClientWrapperFactoryTest {

  @Mock MongoClientFactory mongoClientFactory;
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
            .set( MongoProp.PASSWORD, "password" ).build(),
        logger );
    assertThat( wrapper, instanceOf( UsernamePasswordMongoClientWrapper.class ) );

    wrapper = MongoClientWrapperFactory
        .createMongoClientWrapper(
            new MongoProperties.Builder()
                .set( MongoProp.USE_KERBEROS, "false" )
                .build(),
            logger );
    assertThat( wrapper, instanceOf( NoAuthMongoClientWrapper.class ) );
  }
}
