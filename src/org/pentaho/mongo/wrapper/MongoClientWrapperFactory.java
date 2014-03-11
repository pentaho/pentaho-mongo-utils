package org.pentaho.mongo.wrapper;

import java.lang.reflect.Proxy;

import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.Util;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;

public class MongoClientWrapperFactory {
  public static MongoClientWrapper createMongoClientWrapper( MongoProperties props, MongoUtilLogger log )
    throws MongoDbException {
    if ( Boolean.parseBoolean( props.get( MongoProp.USE_KERBEROS ) ) ) {
      KerberosMongoClientWrapper wrapper = new KerberosMongoClientWrapper( props, log );
      return (MongoClientWrapper) Proxy.newProxyInstance( wrapper.getClass().getClassLoader(),
        new Class<?>[] { MongoClientWrapper.class },
        new KerberosInvocationHandler( wrapper.getAuthContext(), wrapper ) );
    } else if ( !Util.isEmpty( props.get( MongoProp.USER ) ) || !Util.isEmpty( props.get( MongoProp.PASSWORD ) ) ) {
      return new UsernamePasswordMongoClientWrapper( props, log );
    } else {
      return new NoAuthMongoClientWrapper( props, log );
    }
  }
}
