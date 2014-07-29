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
