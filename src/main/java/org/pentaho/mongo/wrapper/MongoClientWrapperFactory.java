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

import java.lang.reflect.Proxy;

import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.Util;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;

/**
 * MongoClientWrapperFactory is used to instantiate MongoClientWrapper objects
 * appropriate for given configuration properties, i.e. using the correct
 * authentication mechanism, server info, and MongoConfigurationOptions.
 */
public class MongoClientWrapperFactory {

  /**
   *
   * @param props  The MongoProperties to use for connection initialization
   * @param log MongoUtilLogger implementation used for all log output
   * @return MongoClientWrapper
   * @throws MongoDbException
   */
  public static MongoClientWrapper createMongoClientWrapper(
    MongoProperties props, MongoUtilLogger log )
    throws MongoDbException {
    if ( props.useKerberos() ) {
      return initKerberosProxy( new KerberosMongoClientWrapper( props, log ) );
    } else if ( !Util.isEmpty( props.get( MongoProp.USERNAME ) ) || !Util.isEmpty( props.get( MongoProp.PASSWORD ) ) ) {
      return new UsernamePasswordMongoClientWrapper( props, log );
    }
    // default
    return new NoAuthMongoClientWrapper( props, log );
  }

  private static MongoClientWrapper initKerberosProxy(
    KerberosMongoClientWrapper wrapper ) {
    return (MongoClientWrapper) Proxy.newProxyInstance( wrapper.getClass().getClassLoader(),
      new Class<?>[] { MongoClientWrapper.class },
      new KerberosInvocationHandler( wrapper.getAuthContext(), wrapper ) );
  }
}
