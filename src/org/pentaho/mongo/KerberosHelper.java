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
   * The variable name that may specify the authentication mode to use when creating a JAAS LoginContext. See {@link
   * JaasAuthenticationMode} for possible values.
   */
  private static final String PENTAHO_JAAS_AUTH_MODE = "PENTAHO_JAAS_AUTH_MODE";

  /**
   * The variable name that may specify the location of the keytab file to use when authenticating with
   * "KERBEROS_KEYTAB" mode.
   */
  private static final String PENTAHO_JAAS_KEYTAB_FILE = "PENTAHO_JAAS_KEYTAB_FILE";

  /**
   * Determine the authentication mode to use based on the property "PENTAHO_JAAS_AUTH_MODE". If not provided this
   * defaults to {@link JaasAuthenticationMode#KERBEROS_USER}.
   *
   * @return The authentication mode to use when creating JAAS {@link LoginContext}s.
   */
  private static JaasAuthenticationMode lookupLoginAuthMode() {
    JaasAuthenticationMode authMode;
    //    try {
    //      // TODO:  handle custom setting of auth mode.
    //      authMode = JaasAuthenticationMode.valueOf(varSpace.getVariable(PENTAHO_JAAS_AUTH_MODE));
    //    } catch (Exception ex) {
    //      // Ignore and use default of USER
    //      authMode = JaasAuthenticationMode.KERBEROS_USER;
    //    }
    authMode = JaasAuthenticationMode.KERBEROS_USER;
    return authMode;
  }

  /**
   * Determine the keytab file to use based on the variable "PENTAHO_JAAS_KEYTAB_FILE". If not is set keytab
   * authentication will not be used.
   *
   * @return keytab file location if defined as the variable "PENTAHO_JAAS_KEYTAB_FILE".
   */
  private static String lookupKeytabFile() {
    return null; //TODO. varSpace.getVariable(PENTAHO_JAAS_KEYTAB_FILE);
  }

  /**
   * Log in to Kerberos with the principal using the configuration defined in the variable space provided.
   *
   * @param principal Principal to log in as.
   * @return The context for the logged in principal.
   * @throws MongoDbException if an error occurs while logging in.
   */
  public static LoginContext login( String principal ) throws MongoDbException {
    try {
      JaasAuthenticationMode authMode = lookupLoginAuthMode();
      String keytabFile = null;  //TODO.  lookupKeytabFile(varSpace);
      return KerberosUtil.loginAs( authMode, principal, keytabFile );
    } catch ( LoginException ex ) {
      throw new MongoDbException( "Unable to authenticate as '" + principal + "'", ex );
    }
  }
}
