package org.pentaho.mongo.wrapper;

import com.mongodb.MongoClient;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.mongo.MongoUtilLogger;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NoAuthMongoClientWrapperTest {

  private MongoClient mockMongoClient;
  private MongoUtilLogger mockMongoUtilLogger;
  private NoAuthMongoClientWrapper noAuthMongoClientWrapper;

  @Before
  public void setUp() throws Exception {
    mockMongoClient = mock( MongoClient.class );
    mockMongoUtilLogger = mock( MongoUtilLogger.class );
    noAuthMongoClientWrapper = new NoAuthMongoClientWrapper( mockMongoClient, mockMongoUtilLogger );
  }

  @Test
  public void testPerform() throws Exception {
    MongoDBAction mockMongoDBAction = mock( MongoDBAction.class );
    noAuthMongoClientWrapper.perform( "Test", mockMongoDBAction );
    verify( mockMongoDBAction, times( 1 ) ).perform( noAuthMongoClientWrapper.getDb( "Test" ) );
  }
}
