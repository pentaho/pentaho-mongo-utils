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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class KerberosUtilTest {

  @Test
  public void testJaasAuthenticationMode() throws MongoDbException {
    assertEquals(
      KerberosUtil.JaasAuthenticationMode.EXTERNAL,
      KerberosUtil.JaasAuthenticationMode.byName( "EXTERNAL" ) );
    assertEquals(
      KerberosUtil.JaasAuthenticationMode.KERBEROS_KEYTAB,
      KerberosUtil.JaasAuthenticationMode.byName( "KERBEROS_KEYTAB" ) );
    assertEquals(
      KerberosUtil.JaasAuthenticationMode.KERBEROS_USER,
      KerberosUtil.JaasAuthenticationMode.byName( "KERBEROS_USER" ) );
    // KERBEROS_USER should be default
    assertEquals(
      KerberosUtil.JaasAuthenticationMode.KERBEROS_USER,
      KerberosUtil.JaasAuthenticationMode.byName( null ) );

    try {
      KerberosUtil.JaasAuthenticationMode.byName( "Invalid" );
      fail();
    } catch ( MongoDbException e ) {
      assertEquals( "PENTAHO_JAAS_AUTH_MODE is incorrect.  "
        + "Should be one of [KERBEROS_USER, KERBEROS_KEYTAB, EXTERNAL], found 'Invalid'.",
        e.getMessage() );
    }
  }

}
