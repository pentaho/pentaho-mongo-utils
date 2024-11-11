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
