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

import com.mongodb.MongoClientOptions;

public enum MongoProp {
  // TODO - should we allow multiple user/pass/db?  Credentials are formed of all three, and are passed as a list.
  // elsewhere in the api we allow specifying a dbname, which may or may not be accounted for in credentials.
  USERNAME,
  PASSWORD,
  DBNAME,

  HOST,
  PORT,
  COLLECTION,
  JOURNALED,
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
  PENTAHO_JAAS_KEYTAB_FILE,

  //MongoClientOptions values
  connectionsPerHost {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption ) {
      builder.connectionsPerHost( propToOption.intValue( props.get( connectionsPerHost ), 100 ) );
    }
  },
  connectTimeout {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption ) {
      builder.connectTimeout( propToOption.intValue( props.get( connectTimeout ), 10000 ) );
    }
  },
  maxAutoConnectRetryTime {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption ) {
      builder.maxAutoConnectRetryTime( propToOption.longValue( props.get( maxAutoConnectRetryTime ), 1000l ) );
    }
  },
  maxWaitTime {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption ) {
      builder.maxWaitTime( propToOption.intValue( props.get( maxWaitTime ), 120000 ) );
    }
  },
  alwaysUseMBeans {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption ) {
      builder.alwaysUseMBeans( propToOption.boolValue( props.get( alwaysUseMBeans ), false ) );
    }
  },
  autoConnectRetry {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption ) {
      builder.autoConnectRetry( propToOption.boolValue( props.get( autoConnectRetry ), false ) );
    }
  },
  cursorFinalizerEnabled {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption ) {
      builder.cursorFinalizerEnabled( propToOption.boolValue( props.get( cursorFinalizerEnabled ), true ) );
    }
  },
  socketKeepAlive {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption ) {
      builder.socketKeepAlive( propToOption.boolValue( props.get( socketKeepAlive ), false ) );
    }
  },
  socketTimeout {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption ) {
      builder.socketTimeout( propToOption.intValue( props.get( socketTimeout ), 0 ) );
    }
  },
  readPreference {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption )
      throws MongoDbException {
      builder.readPreference( propToOption.readPrefValue( props ) );
    }
  },
  tagSet,
  writeConcern {
    @Override
    public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption )
      throws MongoDbException {
      builder.writeConcern( propToOption.writeConcernValue( props ) );
    }
  },
  wTimeout;

  public void setOption( MongoClientOptions.Builder builder, MongoProperties props, MongoPropToOption propToOption )
    throws MongoDbException {
    //default is do nothing since some of the Props are not a MongoClientOption
  }
}
