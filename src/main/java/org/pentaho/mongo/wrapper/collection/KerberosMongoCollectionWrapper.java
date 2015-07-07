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
