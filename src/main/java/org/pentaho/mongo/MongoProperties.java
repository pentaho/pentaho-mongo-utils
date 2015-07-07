/*!
 * Copyright 2010 - 2014 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.mongo;

import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;

import java.util.HashMap;
import java.util.Map;

import static org.pentaho.mongo.MongoProp.readPreference;

/**
 * A container for all properties associated with a MongoClientWrapper, including
 * properties for handling credentials, server lists, and MongoClientOptions.
 * MongoProperties objects are immutable and constructed via a
 * MongoProperties.Builder.
 */
public class MongoProperties {

  private final Map<MongoProp, String> props;

  private MongoProperties( Map<MongoProp, String> props ) {
    this.props = props;
  }

  /**
   * @return the value associated with prop, or null if unset.
   */
  public String get( MongoProp prop ) {
    return props.get( prop );
  }

  /**
   * Constructs MongoClientOptions from the relevant set of properties.
   * See the descriptions of each property in {@link MongoProp}
   * @param log
   * @return
   * @throws MongoDbException
   */
  public MongoClientOptions buildMongoClientOptions( MongoUtilLogger log ) throws MongoDbException {
    MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
    MongoPropToOption propToOption = new MongoPropToOption( log );
    for ( MongoProp prop : MongoProp.values() ) {
      prop.setOption( builder, this, propToOption );
    }
    return builder.build();
  }

  /**
   * Convenience method to determine the boolean property USE_KERBEROS.
   */
  public boolean useKerberos() {
    return Boolean.parseBoolean( props.get( MongoProp.USE_KERBEROS ) );
  }

  /**
   * Convenience method to determine the boolean property USE_ALL_REPLICA_SET_MEMBERS.
   */
  public boolean useAllReplicaSetMembers() {
    return Boolean.valueOf( props.get( MongoProp.USE_ALL_REPLICA_SET_MEMBERS ) );
  }

  /**
   * @return the com.mongodb.ReadPreference associated with the MongoProp.readPreference value.
   */
  public ReadPreference getReadPreference() {
    return ReadPreference.valueOf( props.get( readPreference ) );
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append( "MongoProperties:\n" );
    for ( MongoProp prop : props.keySet() ) {
      builder.append( String.format( "%s=%s\n", prop.name(), props.get( prop ) ) );
    }
    return builder.toString();
  }

  /**
   * Used for constructing MongoProperties.
   */
  public static class Builder {
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_READ_PREFERENCE = "primary";

    private final Map<MongoProp, String> props = new HashMap<MongoProp, String>();

    /**
     * Initializes any default values.
     */
    public Builder() {
      props.put( MongoProp.PASSWORD, "" );
      props.put( MongoProp.HOST, DEFAULT_HOST );
      props.put( MongoProp.readPreference, DEFAULT_READ_PREFERENCE );
    }

    public Builder set( MongoProp prop, String value ) {
      props.put( prop, value );
      return this;
    }

    public MongoProperties build() {
      return new MongoProperties( new HashMap<MongoProp, String>( props ) );
    }
  }
}
