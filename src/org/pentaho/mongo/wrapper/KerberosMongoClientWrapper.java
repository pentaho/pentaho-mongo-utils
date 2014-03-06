package org.pentaho.mongo.wrapper;

import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.KerberosHelper;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.collection.KerberosMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

public class KerberosMongoClientWrapper extends UsernamePasswordMongoClientWrapper {
  private final AuthContext authContext;

  public KerberosMongoClientWrapper( MongoProperties props, MongoUtilLogger log ) throws
    MongoDbException {
    super( props, log );
    authContext = new AuthContext( KerberosHelper.login( getUser() ) );
  }

  public KerberosMongoClientWrapper( MongoClient client,
                                     MongoUtilLogger log,
                                     String username, AuthContext authContext ) {
    super( client, log, username, null );
    this.authContext = authContext;
  }

  @Override
  protected MongoCredential getCredential( MongoProperties props ) {
    return MongoCredential.createGSSAPICredential(
      props.get( MongoProp.USER ) );
  }

  @Override
  protected void authenticateWithDb( DB db ) throws MongoDbException {
    // noop
  }

  @Override
  protected MongoCollectionWrapper wrap( DBCollection collection ) {
    return KerberosInvocationHandler.wrap( MongoCollectionWrapper.class, authContext,
        new KerberosMongoCollectionWrapper( collection, authContext ) );
  }

  public AuthContext getAuthContext() {
    return authContext;
  }
}
