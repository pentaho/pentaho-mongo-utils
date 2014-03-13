package org.pentaho.mongo;

import com.mongodb.MongoClientOptions;

import java.util.HashMap;
import java.util.Map;

public class MongoProperties {

  private final Map<MongoProp, String> props = new HashMap<MongoProp, String>();

  public MongoProperties() {
    // defaults
    props.put( MongoProp.PASSWORD, "" );
    props.put( MongoProp.readPreference, "PRIMARY" );
  }

  public MongoProperties set( MongoProp prop, String value ) {
    props.put( prop, value );
    return this;
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
}
