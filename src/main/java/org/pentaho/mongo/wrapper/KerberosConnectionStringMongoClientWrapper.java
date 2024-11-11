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

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.KerberosHelper;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.wrapper.collection.KerberosMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import javax.security.auth.login.LoginContext;

public class KerberosConnectionStringMongoClientWrapper extends ConnectionStringMongoClientWrapper {
  private final AuthContext authContext;
  MongoProperties props;
  /**
   * Create a connection to a Mongo server based on parameters supplied in the step meta data
   *
   * @param connectionString connectionString to use
   * @param log              for logging
   * @throws MongoDbException if a problem occurs
   */
  KerberosConnectionStringMongoClientWrapper( String connectionString, MongoUtilLogger log ) throws MongoDbException {
    super( connectionString, log );
    this.authContext = getAuthContext( props );
  }

  private MongoProperties getMongoProperties( String connectionString ) {
    String principal = connectionString.substring( connectionString.indexOf( "://" ) + 3, connectionString.indexOf( '@' ) );

    return new MongoProperties.Builder()
                .set( MongoProp.USERNAME, principal )
                .set( MongoProp.PENTAHO_JAAS_AUTH_MODE, "KERBEROS_USER" ).build();

  }
  private AuthContext getAuthContext( MongoProperties props ) throws MongoDbException {
    if ( authContext == null ) {
      return new AuthContext( initLoginContext( props ) );
    }
    return authContext;
  }
  public AuthContext getAuthContext() {
    return authContext;
  }

  private LoginContext initLoginContext( MongoProperties props ) throws MongoDbException {
    return KerberosHelper.login( props.get( MongoProp.USERNAME ), props );
  }
  @Override
  public MongoClientFactory getClientFactory( String connectionString ) {
    try {
      this.props = getMongoProperties( connectionString );
      return KerberosInvocationHandler
         .wrap( MongoClientFactory.class, getAuthContext( props ), new DefaultMongoClientFactory() );
    } catch ( MongoDbException e ) {
      return super.getClientFactory( connectionString );
    }
  }
  @Override
  public MongoClient getMongo( ) {
    return super.getMongo();
  }


  @Override
  protected MongoCollectionWrapper wrap( DBCollection collection ) {
    return KerberosInvocationHandler.wrap( MongoCollectionWrapper.class, authContext,
            new KerberosMongoCollectionWrapper( collection, authContext ) );
  }
}
