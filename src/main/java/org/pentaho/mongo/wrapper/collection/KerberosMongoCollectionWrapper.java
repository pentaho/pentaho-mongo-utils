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

import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.wrapper.KerberosInvocationHandler;
import org.pentaho.mongo.wrapper.cursor.KerberosMongoCursorWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

public class KerberosMongoCollectionWrapper extends DefaultMongoCollectionWrapper {
  private final AuthContext authContext;

  public KerberosMongoCollectionWrapper( DBCollection collection, AuthContext authContext ) {
    super( collection );
    this.authContext = authContext;
  }

  @Override
  protected MongoCursorWrapper wrap( DBCursor cursor ) {
    return KerberosInvocationHandler.wrap( MongoCursorWrapper.class, authContext, new KerberosMongoCursorWrapper(
        cursor, authContext ) );
  }
}
