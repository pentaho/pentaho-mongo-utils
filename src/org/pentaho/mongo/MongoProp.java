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
  USE_KERBEROS
}
