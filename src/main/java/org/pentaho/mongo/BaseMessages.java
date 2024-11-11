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

package org.pentaho.mongo;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class BaseMessages {

  protected static final String BUNDLE_NAME = "messages.messages";

  public static String getString( Class<?> pkg, String key ) {
    return getString( pkg, key, new String[0] );
  }

  public static String getString( Class<?> pkg, String key, String ...parameters ) {
    String packageName = pkg.getPackage().getName();
    try {
      ResourceBundle bundle = ResourceBundle.getBundle(
        packageName + "." + BUNDLE_NAME );
      return MessageFormat.format( bundle.getString( key ), parameters );
    } catch ( IllegalArgumentException e ) {
      String message =
        "Format problem with key=["
          + key + "], locale=[" + Locale.getDefault() + "], package="
          + packageName + " : " + e.toString();
      throw new MissingResourceException( message, packageName, key );
    }
  }
}
