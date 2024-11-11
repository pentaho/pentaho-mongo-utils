/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.mongo;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class AuthContextTest {

  @Test public void testDoPrivalegedActionAsCurrentUser() throws Exception {
    PrivilegedAction<Object> privAction = mock( PrivilegedAction.class );
    AuthContext authContext = new AuthContext( null );
    authContext.doAs( privAction );
    verify( privAction ).run();
  }

  @Test public void testDoPrivalegedActionExceptionAsCurrentUser() throws Exception {
    PrivilegedExceptionAction<Object> privExcAction = mock( PrivilegedExceptionAction.class );
    AuthContext authContext = new AuthContext( null );
    authContext.doAs( privExcAction );
    verify( privExcAction ).run();
  }

  @Test public void testDoPrivalegedActionExceptionThrowsAsCurrentUser() throws Exception {
    PrivilegedExceptionAction<Object> privExcAction = mock( PrivilegedExceptionAction.class );
    Exception mock = mock( RuntimeException.class );
    doThrow( mock ).when( privExcAction ).run();
    AuthContext authContext = new AuthContext( null );
    try {
      authContext.doAs( privExcAction );
      fail();
    } catch ( Exception e ) {
      assertThat( e, CoreMatchers.instanceOf( PrivilegedActionException.class ) );
    }
    verify( privExcAction ).run();
  }

}
