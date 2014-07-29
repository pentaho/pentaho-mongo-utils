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

package org.pentaho.mongo.wrapper;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class KerberosMongoClientWrapperTest {
  @SuppressWarnings( "unchecked" )
  @Test
  public void testWrapProperlyWrapsCollection() throws MongoDbException, PrivilegedActionException {
    MongoClient client = mock( MongoClient.class );
    AuthContext authContext = mock( AuthContext.class );
    MongoUtilLogger log = mock( MongoUtilLogger.class );
    final DBCollection dbCollection = mock( DBCollection.class );
    String username = "test";
    final KerberosMongoClientWrapper wrapper = new KerberosMongoClientWrapper( client, log, username, authContext );
    MongoCollectionWrapper mongoCollectionWrapper = wrapper.wrap( dbCollection );
    when( authContext.doAs( any( PrivilegedExceptionAction.class ) ) ).thenAnswer( new Answer<Void>() {

      @Override
      public Void answer( InvocationOnMock invocation ) throws Throwable {
        dbCollection.drop();
        return null;
      }
    } );
    mongoCollectionWrapper.drop();
    verify( authContext, times( 1 ) ).doAs( any( PrivilegedExceptionAction.class ) );
    verify( dbCollection, times( 1 ) ).drop();
  }
}
