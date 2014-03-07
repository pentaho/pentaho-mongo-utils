package org.pentaho.mongo;

import java.util.HashMap;
import java.util.Map;

public class MongoProperties {

  private final Map<MongoProp, String> props = new HashMap<MongoProp, String>();

  public MongoProperties set( MongoProp prop, String value ) {
    props.put( prop, value );
    return this;
  }

  public String get( MongoProp prop ) {
    return props.get( prop );
  }

}
