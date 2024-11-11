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
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import java.util.List;

public class DefaultMongoClientFactory implements MongoClientFactory {

  public MongoClient getMongoClient(
      List<ServerAddress> serverAddressList, List<MongoCredential> credList,
      MongoClientOptions opts, boolean useReplicaSet ) {
    // Mongo's java driver will discover all replica set or shard
    // members (Mongos) automatically when MongoClient is constructed
    // using a list of ServerAddresses. The javadocs state that MongoClient
    // should be constructed using a SingleServer address instance (rather
    // than a list) when connecting to a stand-alone host - this is why
    // we differentiate here between a list containing one ServerAddress
    // and a single ServerAddress instance via the useAllReplicaSetMembers
    // flag.
    return useReplicaSet || serverAddressList.size() > 1
        ? new MongoClient( serverAddressList, credList, opts )
        : new MongoClient( serverAddressList.get( 0 ), credList, opts );
  }

  @Override
  public MongoClient getConnectionStringMongoClient( String connectionString ) {
    MongoClientURI uri = new MongoClientURI( connectionString );
    return new MongoClient( uri );
  }
}
