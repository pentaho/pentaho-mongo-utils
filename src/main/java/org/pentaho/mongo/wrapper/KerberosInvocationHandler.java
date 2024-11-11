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


package org.pentaho.mongo.wrapper;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.MongoDbException;

/**
 * Handles proxying all method calls through an AuthContext.  This allows methods
 * to be executed as an authenticated user via a LoginContext.
 */
public class KerberosInvocationHandler implements InvocationHandler {
  private final AuthContext authContext;
  private final Object delegate;

  public KerberosInvocationHandler( AuthContext authContext, Object delegate ) {
    this.authContext = authContext;
    this.delegate = delegate;
  }

  @Override
  public Object invoke( Object proxy, final Method method, final Object[] args ) throws MongoDbException {
    try {
      return authContext.doAs( new PrivilegedExceptionAction<Object>() {

        @Override
        public Object run() throws Exception {
          try {
            return method.invoke( delegate, args );
          } catch ( InvocationTargetException e ) {
            Throwable cause = e.getCause();
            if ( cause instanceof Exception ) {
              throw (Exception) cause;
            }
            throw e;
          }
        }
      } );
    } catch ( PrivilegedActionException e ) {
      if ( e.getCause() instanceof MongoDbException ) {
        throw (MongoDbException) e.getCause();
      } else {
        throw new MongoDbException( e.getCause() );
      }
    }
  }

  @SuppressWarnings( "unchecked" )
  public static <T> T wrap( Class<T> iface, AuthContext authContext, Object delegate ) {
    return (T) Proxy.newProxyInstance( KerberosInvocationHandler.class.getClassLoader(), new Class<?>[] { iface },
        new KerberosInvocationHandler( authContext, delegate ) );
  }
}
