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

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of MongoClientWrapper which uses no credentials. Should only be instantiated by
 * MongoClientWrapperFactory.
 */
class UsernamePasswordMongoClientWrapper extends NoAuthMongoClientWrapper {
  private final String user;

  /**
   * Create a connection to a Mongo server based on parameters supplied in the step meta data
   *
   * @param props properties to use
   * @param log   for logging
   * @throws MongoDbException if a problem occurs
   */
  UsernamePasswordMongoClientWrapper( MongoProperties props, MongoUtilLogger log )
    throws MongoDbException {
    super( props, log );
    user = props.get( MongoProp.USERNAME );
  }

  UsernamePasswordMongoClientWrapper( MongoClient mongo, MongoUtilLogger log, String user ) {
    super( mongo, null, log );
    props = null;
    this.user = user;
  }

  public String getUser() {
    return user;
  }

  /**
   * Create a credentials object
   *
   * @return a configured MongoCredential object
   */
  @Override
  public List<MongoCredential> getCredentialList() {
    List<MongoCredential> credList = new ArrayList<MongoCredential>();
    String authDatabase = props.get( MongoProp.AUTH_DATABASE );
    String authMecha = props.get( MongoProp.AUTH_MECHA );
    //if not value on AUTH_MECHA set "MONGODB-CR" default authentication mechanism
    if ( authMecha == null ) {
      authMecha = "";
    }

    //Use the AuthDatabase if one was supplied, otherwise use the connecting database
    authDatabase = ( authDatabase == null || authDatabase.trim().isEmpty() )
                      ? props.get( MongoProp.DBNAME ) : authDatabase;

    if ( authMecha.equalsIgnoreCase( "SCRAM-SHA-1" ) ) {
      credList.add( MongoCredential.createScramSha1Credential(
        props.get( MongoProp.USERNAME ),
        authDatabase,
        props.get( MongoProp.PASSWORD ).toCharArray() ) );
    } else if ( authMecha.equalsIgnoreCase( "PLAIN" ) ) {
      credList.add( MongoCredential.createPlainCredential(
          props.get( MongoProp.USERNAME ),
          authDatabase,
          props.get( MongoProp.PASSWORD ).toCharArray() ) );
    } else {
      credList.add( MongoCredential.createCredential(
        props.get( MongoProp.USERNAME ),
        authDatabase,
        props.get( MongoProp.PASSWORD ).toCharArray() ) );
    }
    return credList;
  }
}
