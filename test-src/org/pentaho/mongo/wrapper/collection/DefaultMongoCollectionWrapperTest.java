package org.pentaho.mongo.wrapper.collection;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DefaultMongoCollectionWrapperTest {

  private DefaultMongoCollectionWrapper defaultMongoCollectionWrapper;
  private DBCollection mockDBCollection;

  @Before
  public void setUp() throws Exception {
    mockDBCollection = mock( DBCollection.class );
    defaultMongoCollectionWrapper = new DefaultMongoCollectionWrapper( mockDBCollection );
  }

  @Test
  public void testRemove() throws Exception {
    defaultMongoCollectionWrapper.remove();
    verify( mockDBCollection, times( 1 ) ).remove( eq( new BasicDBObject() ) );
  }
}