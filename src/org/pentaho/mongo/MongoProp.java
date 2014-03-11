/*!
  * Copyright 2010 - 2014 Pentaho Corporation.  All rights reserved.
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

public enum MongoProp {
// TODO - should we allow multiple user/pass/db?  Credentials are formed of all three, and are passed as a list.
  // elsewhere in the api we allow specifying a dbname, which may or may not be accounted for in credentials.
  USER,
  PASSWORD,
  DBNAME,

  HOST,
  PORT,
  COLLECTION,
  CONNECT_TIMEOUT,
  SOCKET_TIMEOUT,
  READ_PREFERENCE,
  WRITE_CONCERN,
  WRITE_TIMEOUT,
  JOURNALED,
  TAG_SET,
  USE_ALL_REPLICA_SET_MEMBERS,
  USE_KERBEROS,

  /**
   * The variable name that may specify the authentication mode to use when creating a JAAS LoginContext. See {@link
   * org.pentaho.mongo.KerberosUtil.JaasAuthenticationMode} for possible values.
   */
  PENTAHO_JAAS_AUTH_MODE,

  /**
   * The variable name that may specify the location of the keytab file to use when authenticating with
   * "KERBEROS_KEYTAB" mode.
   */
  PENTAHO_JAAS_KEYTAB_FILE
}
