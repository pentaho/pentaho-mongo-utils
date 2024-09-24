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

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.pentaho.mongo.KerberosUtil.JaasAuthenticationMode;

/**
 * A collection of helper functions to make working with Kettle and Kerberos easier.
 *
 * @author Jordan Ganoff <jganoff@pentaho.com>
 */
public class KerberosHelper {

  /**
   * Determine the authentication mode to use based on the property "PENTAHO_JAAS_AUTH_MODE". If not provided this
   * defaults to {@link JaasAuthenticationMode#KERBEROS_USER}.
   *
   * @return The authentication mode to use when creating JAAS {@link LoginContext}s.
   * @param props properties for this connection
   */
  private static JaasAuthenticationMode lookupLoginAuthMode( MongoProperties props ) throws MongoDbException {
    return JaasAuthenticationMode.byName( props.get( MongoProp.PENTAHO_JAAS_AUTH_MODE ) );
  }

  /**
   * Determine the keytab file to use based on the variable "PENTAHO_JAAS_KEYTAB_FILE". If not is set keytab
   * authentication will not be used.
   *
   * @return keytab file location if defined as the variable "PENTAHO_JAAS_KEYTAB_FILE".
   * @param props properties for this connection
   */
  private static String lookupKeytabFile( MongoProperties props ) {
    return props.get( MongoProp.PENTAHO_JAAS_KEYTAB_FILE );
  }

  /**
   * Log in to Kerberos with the principal using the configuration defined in the variable space provided.
   *
   *
   * @param principal Principal to log in as.
   * @param props properties for this connection
   * @return The context for the logged in principal.
   * @throws MongoDbException if an error occurs while logging in.
   */
  public static LoginContext login( String principal, MongoProperties props ) throws MongoDbException {
    try {
      JaasAuthenticationMode authMode = lookupLoginAuthMode( props );
      String keytabFile =  lookupKeytabFile( props );
      return KerberosUtil.loginAs( authMode, principal, keytabFile );
    } catch ( LoginException ex ) {
      throw new MongoDbException( "Unable to authenticate as '" + principal + "'", ex );
    }
  }
}
