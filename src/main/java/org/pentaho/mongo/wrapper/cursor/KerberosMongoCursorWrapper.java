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


package org.pentaho.mongo.wrapper.cursor;

import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.wrapper.KerberosInvocationHandler;

import com.mongodb.DBCursor;

public class KerberosMongoCursorWrapper extends DefaultCursorWrapper {
  private final AuthContext authContext;

  public KerberosMongoCursorWrapper( DBCursor cursor, AuthContext authContext ) {
    super( cursor );
    this.authContext = authContext;
  }

  @Override
  protected MongoCursorWrapper wrap( DBCursor cursor ) {
    return KerberosInvocationHandler.wrap( MongoCursorWrapper.class, authContext, new KerberosMongoCursorWrapper(
        cursor, authContext ) );
  }

}
