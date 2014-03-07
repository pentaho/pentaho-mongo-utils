package org.pentaho.mongo.wrapper.org.pentaho.functional;

import junit.framework.TestCase;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class TestBase extends TestCase {


  protected final Properties testProperties = initTestProperties();

  protected Properties initTestProperties() {
    Properties props = new Properties();
    try {
      props.load(
        new FileReader( "test.properties" )  );
      return props;
    } catch ( IOException e ) {
      throw new RuntimeException( e );
    }
  }


}
