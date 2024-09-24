/*!
* Copyright 2010 - 2017 Hitachi Vantara.  All rights reserved.
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
