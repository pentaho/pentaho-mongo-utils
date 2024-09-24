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

package org.pentaho.mongo.wrapper.collection;

import com.mongodb.AggregationOptions;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultMongoCollectionWrapperTest {

  private DefaultMongoCollectionWrapper defaultMongoCollectionWrapper;
  @Mock private DBCollection mockDBCollection;
  @Mock private BasicDBObject dbObject;
  @Mock private List<DBObject> dbObjList;

  private DBObject[] dbObjectArray = new DBObject[0];

  @Before public void setUp() throws Exception {
    MockitoAnnotations.initMocks( this );
    defaultMongoCollectionWrapper = new DefaultMongoCollectionWrapper( mockDBCollection );
  }

  @Test public void testRemove() throws Exception {
    defaultMongoCollectionWrapper.remove();
    verify( mockDBCollection, times( 1 ) ).remove( eq( new BasicDBObject() ) );
  }

  @Test public void testCreateIndex() throws Exception {
    BasicDBObject index = mock( BasicDBObject.class );
    BasicDBObject options = mock( BasicDBObject.class );
    defaultMongoCollectionWrapper.createIndex( index, options );
    verify( mockDBCollection ).createIndex( index, options );
  }

  @Test public void testPassThroughMethods() throws MongoDbException {
    // Setup aggregate to use MongoDB Cursor method instead
    AggregationOptions options = AggregationOptions.builder().build();
    List<DBObject> pipeline = new ArrayList<>(); // can be empty

    defaultMongoCollectionWrapper.drop();
    verify( mockDBCollection ).drop();
    defaultMongoCollectionWrapper.aggregate( pipeline, options );
    verify( mockDBCollection ).aggregate( pipeline, options );
    defaultMongoCollectionWrapper.update( dbObject, dbObject, true, true );
    verify( mockDBCollection ).update( dbObject, dbObject, true, true );
    defaultMongoCollectionWrapper.insert( dbObjList );
    verify( mockDBCollection ).insert( dbObjList );
    defaultMongoCollectionWrapper.dropIndex( dbObject );
    verify( mockDBCollection ).dropIndex( dbObject );
    defaultMongoCollectionWrapper.createIndex( dbObject );
    verify( mockDBCollection ).createIndex( dbObject );
    defaultMongoCollectionWrapper.save( dbObject );
    verify( mockDBCollection ).save( dbObject );
    defaultMongoCollectionWrapper.count();
    verify( mockDBCollection ).count();
    defaultMongoCollectionWrapper.distinct( "key" );
    verify( mockDBCollection ).distinct( "key" );
  }

  @Test
  public void testAggregate() throws MongoDbException {
    Cursor mockCursor = mock( Cursor.class );
    when( mockDBCollection.aggregate( anyList(), any( AggregationOptions.class ) ) ).thenReturn( mockCursor );
    Cursor ret = defaultMongoCollectionWrapper.aggregate( dbObject, dbObjectArray, false );
    assertEquals( mockCursor, ret );
  }

  @Test
  public void testAggregateWithAllowDiskUse() {
    Cursor mockCursor = mock( Cursor.class );
    when( mockDBCollection.aggregate( anyList(), any( AggregationOptions.class ) ) ).thenReturn( mockCursor );
    Cursor ret = defaultMongoCollectionWrapper.aggregate( dbObject, dbObjectArray, true );
    assertEquals( mockCursor, ret );
  }

  @Test public void testFindWrapsCursor() throws MongoDbException {
    assertThat( defaultMongoCollectionWrapper.find(), CoreMatchers.instanceOf( MongoCursorWrapper.class ) );
    verify( mockDBCollection ).find();
    assertThat( defaultMongoCollectionWrapper.find( dbObject, dbObject ), CoreMatchers.instanceOf( MongoCursorWrapper.class ) );
    verify( mockDBCollection ).find( dbObject, dbObject );
    assertThat( defaultMongoCollectionWrapper.find( dbObject ), CoreMatchers.instanceOf( MongoCursorWrapper.class ) );
    verify( mockDBCollection ).find( dbObject );

  }
}
