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
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.List;

/**
 * User: Dzmitry Stsiapanau Date: 04/06/2016 Time: 12:37
 */
public interface MongoClientFactory {

  MongoClient getMongoClient( List<ServerAddress> serverAddressList, List<MongoCredential> credList,
                              MongoClientOptions opts, boolean useReplicaSet );
  MongoClient getConnectionStringMongoClient( String connectionString );
}
