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

package org.pentaho.mongo;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

/**
 * A context for executing authorized actions on behalf of an authenticated user via a {@link LoginContext}.
 *
 * @author Jordan Ganoff <jganoff@pentaho.com>
 */
public class AuthContext {
  private LoginContext login;

  /**
   * Create a context for the given login. If the login is null all operations will be done as the current user.
   * <p/>
   * TODO Prevent null login contexts and create login contexts for the current OS user instead. This will keep the
   * implementation cleaner.
   *
   * @param login
   */
  public AuthContext( LoginContext login ) {
    this.login = login;
  }

  /**
   * Execute an action on behalf of the login used to create this context. If no user is explicitly authenticated the
   * action will be executed as the current user.
   *
   * @param action The action to execute
   * @return The return value of the action
   */
  public <T> T doAs( PrivilegedAction<T> action ) {
    if ( login == null ) {
      // If a user is not explicitly authenticated directly execute the action
      return action.run();
    } else {
      return Subject.doAs( login.getSubject(), action );
    }
  }

  /**
   * Execute an action on behalf of the login used to create this context. If no user is explicitly authenticated the
   * action will be executed as the current user.
   *
   * @param action The action to execute
   * @return The return value of the action
   * @throws PrivilegedActionException If an exception occurs while executing the action. The cause of the exception
   *                                   will be provided in {@link PrivilegedActionException#getCause()}.
   */
  public <T> T doAs( PrivilegedExceptionAction<T> action ) throws PrivilegedActionException {
    if ( login == null ) {
      // If a user is not explicitly authenticated directly execute the action
      try {
        return action.run();
      } catch ( Exception ex ) {
        // Wrap any exceptions throw in a PrivilegedActionException just as
        // would be thrown when executed via Subject.doAs(..)
        throw new PrivilegedActionException( ex );
      }
    } else {
      return Subject.doAs( login.getSubject(), action );
    }
  }
}
