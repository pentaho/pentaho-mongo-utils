/*!
 * Copyright 2010 - 2021 Hitachi Vantara.  All rights reserved.
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
