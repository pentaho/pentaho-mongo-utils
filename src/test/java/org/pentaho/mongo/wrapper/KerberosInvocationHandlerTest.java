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

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.MongoDbException;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

public class KerberosInvocationHandlerTest {
  @SuppressWarnings( "unchecked" )
  @Test
  public void testInvocationHandlerCallsDoAsWhichCallsDelegate() throws MongoDbException, PrivilegedActionException {
    final MongoClientWrapper wrapper = mock( MongoClientWrapper.class );
    AuthContext authContext = mock( AuthContext.class );
    MongoClientWrapper wrappedWrapper =
      KerberosInvocationHandler.wrap( MongoClientWrapper.class, authContext, wrapper );
    when( authContext.doAs( any( PrivilegedExceptionAction.class ) ) ).thenAnswer( new Answer<Void>() {

      @Override
      public Void answer( InvocationOnMock invocation ) throws Throwable {
        wrapper.dispose();
        return null;
      }
    } );
    wrappedWrapper.dispose();
    verify( authContext, times( 1 ) ).doAs( any( PrivilegedExceptionAction.class ) );
    verify( wrapper, times( 1 ) ).dispose();
  }
}
