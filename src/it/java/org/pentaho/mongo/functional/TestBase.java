/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.mongo.functional;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestBase {

  protected static final Properties testProperties = initTestProperties();

  protected static Properties initTestProperties() {
    Properties props = new Properties();
    try {
      props.load( TestBase.class.getResourceAsStream( "/test.properties" ) );
      return props;
    } catch ( IOException e ) {
      throw new RuntimeException( e );
    }
  }


}
