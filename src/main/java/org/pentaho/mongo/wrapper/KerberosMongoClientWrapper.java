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
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.KerberosHelper;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.wrapper.collection.KerberosMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import javax.security.auth.login.LoginContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of MongoClientWrapper which uses the GSSAPI auth mechanism. Should only be instantiated by
 * MongoClientWrapperFactory.
 */
class KerberosMongoClientWrapper extends UsernamePasswordMongoClientWrapper {
  private final AuthContext authContext;

  public KerberosMongoClientWrapper( MongoProperties props, MongoUtilLogger log ) throws
    MongoDbException {
    super( props, log );
    authContext = getAuthContext( props );
  }

  private AuthContext getAuthContext( MongoProperties props ) throws MongoDbException {
    if ( authContext == null ) {
      return new AuthContext( initLoginContext( props ) );
    }
    return authContext;
  }

  private LoginContext initLoginContext( MongoProperties props ) throws MongoDbException {
    return KerberosHelper.login( props.get( MongoProp.USERNAME ), props );
  }

  KerberosMongoClientWrapper( MongoClient client, MongoUtilLogger log, String username, AuthContext authContext ) {
    super( client, log, username );
    this.authContext = authContext;
  }

  @Override protected MongoClient getClient( MongoClientOptions opts ) throws MongoDbException {
    return super.getClient( opts );
  }

  @Override
  public List<MongoCredential> getCredentialList() {
    List<MongoCredential> credList = new ArrayList<MongoCredential>();
    credList.add( MongoCredential.createGSSAPICredential(
      props.get( MongoProp.USERNAME ) ) );
    return credList;
  }

  @Override
  protected MongoCollectionWrapper wrap( DBCollection collection ) {
    return KerberosInvocationHandler.wrap( MongoCollectionWrapper.class, authContext,
      new KerberosMongoCollectionWrapper( collection, authContext ) );
  }

  public AuthContext getAuthContext() {
    return authContext;
  }

  @Override public MongoClientFactory getClientFactory( final MongoProperties opts ) {
    try {
      return KerberosInvocationHandler
        .wrap( MongoClientFactory.class, getAuthContext( opts ), new DefaultMongoClientFactory() );
    } catch ( MongoDbException e ) {
      return super.getClientFactory( opts );
    }
  }
}
