package org.pentaho.mongo.wrapper.collection;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

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
    DBObject mockDBObject = mock( DBObject.class );
    defaultMongoCollectionWrapper.remove( mockDBObject );
  }
}