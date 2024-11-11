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

import java.lang.reflect.Proxy;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.Util;
import org.pentaho.mongo.BaseMessages;
/**
 * MongoClientWrapperFactory is used to instantiate MongoClientWrapper objects
 * appropriate for given configuration properties, i.e. using the correct
 * authentication mechanism, server info, and MongoConfigurationOptions.
 */
public class MongoClientWrapperFactory {
  private static Class<?> PKG = MongoClientWrapperFactory.class;

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
    } else if ( !Util.isEmpty( props.get( MongoProp.USERNAME ) )
        || !Util.isEmpty( props.get( MongoProp.PASSWORD ) )
        || !Util.isEmpty( props.get( MongoProp.AUTH_DATABASE ) ) ) {
      return new UsernamePasswordMongoClientWrapper( props, log );
    }
    // default
    return new NoAuthMongoClientWrapper( props, log );
  }
  /**
   *
   * @param connectionString  The ConnectionString to use for connection initialization
   * @param log MongoUtilLogger implementation used for all log output
   * @return MongoClientWrapper
   * @throws MongoDbException
   */
  public static MongoClientWrapper createConnectionStringMongoClientWrapper(
          String connectionString, MongoUtilLogger log )
          throws MongoDbException {
    if ( connectionString == null || connectionString.isEmpty() ) {
      throw new MongoDbException( BaseMessages.getString( PKG, "MongoConnectionStringWrapper.ErrorMessage.EmptyConnectionString" ) ); //$NON-NLS-1$
    }

    if ( connectionString.toLowerCase().contains( "authmechanism=gssapi" ) ) {
      return initConnectionStringKerberosProxy( new KerberosConnectionStringMongoClientWrapper( connectionString, log ) );
    }
    return new ConnectionStringMongoClientWrapper( connectionString, log );
  }

  private static MongoClientWrapper initKerberosProxy(
    KerberosMongoClientWrapper wrapper ) {
    return (MongoClientWrapper) Proxy.newProxyInstance( wrapper.getClass().getClassLoader(),
      new Class<?>[] { MongoClientWrapper.class },
      new KerberosInvocationHandler( wrapper.getAuthContext(), wrapper ) );
  }
  private static MongoClientWrapper initConnectionStringKerberosProxy(
          KerberosConnectionStringMongoClientWrapper wrapper ) {
    return (MongoClientWrapper) Proxy.newProxyInstance( wrapper.getClass().getClassLoader(),
            new Class<?>[] { MongoClientWrapper.class },
            new KerberosInvocationHandler( wrapper.getAuthContext(), wrapper ) );
  }
}
