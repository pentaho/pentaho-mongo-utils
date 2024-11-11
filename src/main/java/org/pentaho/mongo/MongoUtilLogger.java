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


public interface MongoUtilLogger {

  void debug( java.lang.String s );

  void info( java.lang.String s );

  void warn( java.lang.String s, java.lang.Throwable throwable );

  void error( java.lang.String s, java.lang.Throwable throwable );

  boolean isDebugEnabled();

}
