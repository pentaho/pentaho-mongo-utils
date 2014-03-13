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
