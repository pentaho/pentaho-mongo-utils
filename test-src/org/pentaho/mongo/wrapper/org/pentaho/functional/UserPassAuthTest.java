package org.pentaho.mongo.wrapper.org.pentaho.functional;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoClientWrapperFactory;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UserPassAuthTest extends TestBase {

  private final String host = (String) testProperties.get( "userpass.auth.host" );
  private final String port = (String) testProperties.get( "userpass.auth.port" );
  private final String user = (String) testProperties.get( "userpass.auth.user" );
  private final String password = (String) testProperties.get( "userpass.auth.password" );
  private final String db = (String) testProperties.get( "userpass.auth.db" );

  private String tempCollection;
  private MongoClientWrapper clientWrapper;

  @Override protected void setUp() throws Exception {
    super.setUp();
    tempCollection = null;
  }

  @Override protected void tearDown() throws Exception {
    super.tearDown();
    if ( tempCollection != null ) {
      MongoClientWrapper wrapper = getWrapper();
      wrapper.getCollection( db, tempCollection ).drop();
      wrapper.dispose();
    }
    if ( clientWrapper != null ) {
      clientWrapper.dispose();
    }
  }

  @Test
  public void testCreateDropCollection() throws MongoDbException {
    clientWrapper = getWrapper();

    tempCollection = "testCollection" + UUID.randomUUID().toString().replace( "-", "" );

    clientWrapper.createCollection( db, tempCollection );
    MongoCollectionWrapper collectionWrapper = clientWrapper.getCollection( db, tempCollection );

    List<DBObject> dbObjects = new ArrayList<DBObject>();
    dbObjects.add( new BasicDBObject( "foo", "bar" ) );
    collectionWrapper.insert( dbObjects );
    assertEquals( "bar", collectionWrapper.find().next().get( "foo" ) );

    assertEquals( 1, collectionWrapper.count() );
    assertEquals( "bar", collectionWrapper.distinct( "foo" ).get( 0 ) );

    collectionWrapper.drop();
    assertFalse( clientWrapper.getCollectionsNames( db ).contains( tempCollection ) );

    tempCollection = null;
  }

  public void testCursor() throws MongoDbException {
    clientWrapper = getWrapper();
    tempCollection = "testCollection" + UUID.randomUUID().toString().replace( "-", "" );

    clientWrapper.createCollection( db, tempCollection );
    MongoCollectionWrapper collectionWrapper = clientWrapper.getCollection( db, tempCollection );

    List<DBObject> dbObjects = new ArrayList<DBObject>();
    for ( int i = 0; i < 100; i++ ) {
      dbObjects.add( new BasicDBObject( "foo", "bar" + i ) );
    }
    collectionWrapper.insert( dbObjects );

    MongoCursorWrapper cursor = collectionWrapper.find();

    cursor = cursor.limit( 10 );

    int i = 0;
    while ( cursor.hasNext() ) {
      assertEquals( "bar" + i, cursor.next().get( "foo" ) );
      i++;
    }
    assertEquals( "Should be limited to 10 items", 10, i );

    assertEquals( host, cursor.getServerAddress().getHost() );
    assertEquals( Integer.parseInt( port ), cursor.getServerAddress().getPort() );
  }


  private MongoClientWrapper getWrapper() throws MongoDbException {
    return MongoClientWrapperFactory.createMongoClientWrapper(
      new MongoProperties()
        .set( MongoProp.HOST, host )
        .set( MongoProp.PORT, port )
        .set( MongoProp.USERNAME, user )
        .set( MongoProp.PASSWORD, password )
        .set( MongoProp.DBNAME, db ),
      null );
  }


}
