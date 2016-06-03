/*!
  * Copyright 2010 - 2016 Pentaho Corporation.  All rights reserved.
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

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of MongoClientWrapper which uses no credentials.
 * Should only be instantiated by MongoClientWrapperFactory.
 */
class UsernamePasswordMongoClientWrapper extends NoAuthMongoClientWrapper {
  private final String user;

  /**
   * Default authentication database for backward compatibility with pentaho-mongodb-plugin 5.x
   * http://jira.pentaho.com/browse/SP-2700
   */
  private static final String DEFAULT_AUTH_DB = "admin";

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
    String authDbName = props.get( MongoProp.DBNAME );
    credList.add( MongoCredential.createCredential(
        props.get( MongoProp.USERNAME ),
        authDbName == null || authDbName.trim().length() == 0 ? DEFAULT_AUTH_DB : authDbName,
        props.get( MongoProp.PASSWORD ).toCharArray() ) );
    return credList;
  }
}
