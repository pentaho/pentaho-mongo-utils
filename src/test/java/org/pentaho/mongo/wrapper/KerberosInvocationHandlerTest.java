/*!
  * Copyright 2010 - 2014 Pentaho Corporation.  All rights reserved.
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

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.MongoDbException;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static org.mockito.Matchers.any;
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
