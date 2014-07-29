/*! 
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL 
 * 
 * Copyright 2002 - 2014 Pentaho Corporation (Pentaho). All rights reserved. 
 * 
 * NOTICE: All information including source code contained herein is, and 
 * remains the sole property of Pentaho and its licensors. The intellectual 
 * and technical concepts contained herein are proprietary and confidential 
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign 
 * patents, or patents in process, and are protected by trade secret and 
 * copyright laws. The receipt or possession of this source code and/or related 
 * information does not convey or imply any rights to reproduce, disclose or 
 * distribute its contents, or to manufacture, use, or sell anything that it 
 * may describe, in whole or in part. Any reproduction, modification, distribution, 
 * or public display of this information without the express written authorization 
 * from Pentaho is strictly prohibited and in violation of applicable laws and 
 * international treaties. Access to the source code contained herein is strictly 
 * prohibited to anyone except those individuals and entities who have executed 
 * confidentiality and non-disclosure agreements or other agreements with Pentaho, 
 * explicitly covering such access. 
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
