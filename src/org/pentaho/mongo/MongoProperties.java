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

import java.util.HashMap;
import java.util.Map;

public class MongoProperties {

  private final Map<MongoProp, String> props;

  private MongoProperties( Map<MongoProp, String> props ) {
    this.props = props;
  }

  public String get( MongoProp prop ) {
    return props.get( prop );
  }

  public MongoClientOptions buildMongoClientOptions( MongoUtilLogger log ) throws MongoDbException {
    MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
    MongoPropToOption propToOption = new MongoPropToOption( log );
    for ( MongoProp prop : MongoProp.values() ) {
      prop.setOption( builder, this, propToOption );
    }
    return builder.build();
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

  public static class Builder {
    private final Map<MongoProp, String> props = new HashMap<MongoProp, String>();

    public Builder() {
      props.put( MongoProp.PASSWORD, "" );
      props.put( MongoProp.readPreference, "PRIMARY" );
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
