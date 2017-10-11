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

package org.pentaho.mongo.wrapper;

import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.ServerAddress;
import org.pentaho.mongo.BaseMessages;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.Util;
import org.pentaho.mongo.wrapper.collection.DefaultMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of MongoClientWrapper which uses the MONGO-CR auth mechanism.
 * Should only be instantiated by MongoClientWrapperFactory.
 */
class NoAuthMongoClientWrapper implements MongoClientWrapper {
  private static Class<?> PKG = NoAuthMongoClientWrapper.class;
  public static final int MONGO_DEFAULT_PORT = 27017;

  public static final String LOCAL_DB = "local"; //$NON-NLS-1$
  public static final String REPL_SET_COLLECTION = "system.replset"; //$NON-NLS-1$
  public static final String REPL_SET_SETTINGS = "settings"; //$NON-NLS-1$
  public static final String REPL_SET_LAST_ERROR_MODES = "getLastErrorModes"; //$NON-NLS-1$
  public static final String REPL_SET_MEMBERS = "members"; //$NON-NLS-1$

  static MongoClientFactory clientFactory = new DefaultMongoClientFactory();

  private final MongoClient mongo;
  private final MongoUtilLogger log;

  protected MongoProperties props;

  /**
   * Create a connection to a Mongo server based on parameters supplied in the step meta data
   *
   * @param props properties to use
   * @param log   for logging
   * @throws MongoDbException if a problem occurs
   */
  NoAuthMongoClientWrapper( MongoProperties props, MongoUtilLogger log )
    throws MongoDbException {
    this.log = log;
    this.props = props;
    mongo = getClient( props.buildMongoClientOptions( log ) );
  }

  NoAuthMongoClientWrapper(
      MongoClient mongo,
      MongoProperties props,
      MongoUtilLogger log ) {
    this.mongo = mongo;
    this.log = log;
    this.props = props;
  }

  MongoClient getMongo() {
    return mongo;
  }

  private List<ServerAddress> getServerAddressList() throws MongoDbException {
    String hostsPorts = props.get( MongoProp.HOST );
    String singlePort = props.get( MongoProp.PORT );

    int singlePortI = -1;

    try {
      singlePortI = Integer.parseInt( singlePort );
    } catch ( NumberFormatException n ) {
      // don't complain
    }

    if ( Util.isEmpty( hostsPorts ) ) {
      throw new MongoDbException(
        BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Error.EmptyHostsString" ) ); //$NON-NLS-1$
    }

    List<ServerAddress> serverList = new ArrayList<ServerAddress>();

    String[] parts = hostsPorts.trim().split( "," ); //$NON-NLS-1$
    for ( String part : parts ) {
      // host:port?
      int port = singlePortI != -1 ? singlePortI : MONGO_DEFAULT_PORT;
      String[] hp = part.split( ":" ); //$NON-NLS-1$
      if ( hp.length > 2 ) {
        throw new MongoDbException(
          BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Error.MalformedHost", part ) ); //$NON-NLS-1$
      }

      String host = hp[ 0 ];
      if ( hp.length == 2 ) {
        // non-default port
        try {
          port = Integer.parseInt( hp[ 1 ].trim() );
        } catch ( NumberFormatException n ) {
          throw new MongoDbException( BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.Message.Error.UnableToParsePortNumber", hp[ 1 ] ) ); //$NON-NLS-1$
        }
      }

      try {
        ServerAddress s = new ServerAddress( host, port );
        serverList.add( s );
      } catch ( Throwable u ) {
        throw new MongoDbException( u );
      }
    }
    return serverList;
  }


  protected MongoClient getClient( MongoClientOptions opts ) throws MongoDbException {
    List<MongoCredential> credList = getCredentialList();
    List<ServerAddress> serverAddressList = getServerAddressList();

    if ( serverAddressList.size() == 0 ) {
      // There should be minimally one server defined.  Default is localhost, so
      // this can only happen if it's been explicitly set to NULL.
      throw new MongoDbException(
        BaseMessages.getString(
          MongoClientWrapper.class,
          "MongoNoAuthWrapper.Message.Error.NoHostSet" ) );
    }
    return getClientFactory( props )
      .getMongoClient( serverAddressList, credList, opts,
        props.useAllReplicaSetMembers() );
  }


  /**
   * Retrieve all database names found in MongoDB as visible by the authenticated user.
   *
   * @throws MongoDbException
   */
  public List<String> getDatabaseNames() throws MongoDbException {
    try {
      return getMongo().getDatabaseNames();
    } catch ( Exception e ) {
      throw new MongoDbException( e );
    }
  }

  protected DB getDb( String dbName ) throws MongoDbException {
    try {
      return getMongo().getDB( dbName );
    } catch ( Exception e ) {
      throw new MongoDbException( e );
    }
  }

  /**
   * Get the set of collections for a MongoDB database.
   *
   * @param dB Name of database
   * @return Set of collections in the database requested.
   * @throws MongoDbException If an error occurs.
   */
  public Set<String> getCollectionsNames( String dB ) throws MongoDbException {
    try {
      return getDb( dB ).getCollectionNames();
    } catch ( Exception e ) {
      if ( e instanceof MongoDbException ) {
        throw (MongoDbException) e;
      } else {
        throw new MongoDbException( e );
      }
    }
  }

  /**
   * Return a list of custom "lastErrorModes" (if any) defined in the replica set configuration object on the server.
   * These can be used as the "w" setting for the write concern in addition to the standard "w" values of <number> or
   * "majority".
   *
   * @return a list of the names of any custom "lastErrorModes"
   * @throws MongoDbException if a problem occurs
   */
  public List<String> getLastErrorModes() throws MongoDbException {
    List<String> customLastErrorModes = new ArrayList<String>();

    DB local = getDb( LOCAL_DB );
    if ( local != null ) {
      try {
        DBCollection replset = local.getCollection( REPL_SET_COLLECTION );
        if ( replset != null ) {
          DBObject config = replset.findOne();

          extractLastErrorModes( config, customLastErrorModes );
        }
      } catch ( Exception e ) {
        throw new MongoDbException( e );
      }
    }

    return customLastErrorModes;
  }

  protected void extractLastErrorModes( DBObject config, List<String> customLastErrorModes ) {
    if ( config != null ) {
      Object settings = config.get( REPL_SET_SETTINGS );

      if ( settings != null ) {
        Object getLastErrModes = ( (DBObject) settings ).get( REPL_SET_LAST_ERROR_MODES );

        if ( getLastErrModes != null ) {
          for ( String m : ( (DBObject) getLastErrModes ).keySet() ) {
            customLastErrorModes.add( m );
          }
        }
      }
    }
  }

  public List<String> getIndexInfo( String dbName, String collection ) throws MongoDbException {
    try {
      DB db = getDb( dbName );

      if ( db == null ) {
        throw new MongoDbException(
          BaseMessages.getString( PKG, "MongoNoAuthWrapper.ErrorMessage.NonExistentDB", dbName ) ); //$NON-NLS-1$
      }

      if ( Util.isEmpty( collection ) ) {
        throw new MongoDbException(
          BaseMessages.getString( PKG, "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified" ) ); //$NON-NLS-1$
      }

      if ( !db.collectionExists( collection ) ) {
        db.createCollection( collection, null );
      }

      DBCollection coll = db.getCollection( collection );
      if ( coll == null ) {
        throw new MongoDbException( BaseMessages.getString( PKG,
          "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection", //$NON-NLS-1$
          collection ) );
      }

      List<DBObject> collInfo = coll.getIndexInfo();
      List<String> result = new ArrayList<String>();
      if ( collInfo == null || collInfo.size() == 0 ) {
        throw new MongoDbException( BaseMessages.getString( PKG,
          "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection", //$NON-NLS-1$
          collection ) );
      }

      for ( DBObject index : collInfo ) {
        result.add( index.toString() );
      }

      return result;
    } catch ( Exception e ) {
      log.error( BaseMessages.getString( PKG, "MongoNoAuthWrapper.ErrorMessage.GeneralError.Message" ) //$NON-NLS-1$
          + ":\n\n" + e.getMessage(), e ); //$NON-NLS-1$
      if ( e instanceof MongoDbException ) {
        throw (MongoDbException) e;
      } else {
        throw new MongoDbException( e );
      }
    }
  }

  @Override
  public List<String> getAllTags() throws MongoDbException {
    return setupAllTags( getRepSetMemberRecords() );
  }

  private BasicDBList getRepSetMemberRecords() throws MongoDbException {
    BasicDBList setMembers = null;
    try {
      DB local = getDb( LOCAL_DB );
      if ( local != null ) {

        DBCollection replset = local.getCollection( REPL_SET_COLLECTION );
        if ( replset != null ) {
          DBObject config = replset.findOne();

          if ( config != null ) {
            Object members = config.get( REPL_SET_MEMBERS );

            if ( members instanceof BasicDBList ) {
              if ( ( (BasicDBList) members ).size() == 0 ) {
                // log that there are no replica set members defined
                logInfo( BaseMessages.getString( PKG,
                    "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined" ) ); //$NON-NLS-1$
              } else {
                setMembers = (BasicDBList) members;
              }

            } else {
              // log that there are no replica set members defined
              logInfo( BaseMessages.getString( PKG,
                  "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined" ) ); //$NON-NLS-1$
            }
          } else {
            // log that there are no replica set members defined
            logInfo( BaseMessages.getString( PKG,
                "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined" ) ); //$NON-NLS-1$
          }
        } else {
          // log that the replica set collection is not available
          logInfo( BaseMessages.getString( PKG,
              "MongoNoAuthWrapper.Message.Warning.ReplicaSetCollectionUnavailable" ) ); //$NON-NLS-1$
        }
      } else {
        // log that the local database is not available!!
        logInfo(
            BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Warning.LocalDBNotAvailable" ) ); //$NON-NLS-1$
      }
    } catch ( Exception ex ) {
      throw new MongoDbException( ex );
    } finally {
      if ( getMongo() != null ) {
        getMongo().close();
      }
    }

    return setMembers;
  }

  private void logInfo( String message ) {
    if ( log != null ) {
      log.info( message );
    }
  }

  protected List<String> setupAllTags( BasicDBList members ) {
    HashSet<String> tempTags = new HashSet<String>();

    if ( members != null && members.size() > 0 ) {
      for ( Object member : members ) {
        if ( member != null ) {
          DBObject tags = (DBObject) ( (DBObject) member ).get( "tags" ); //$NON-NLS-1$
          if ( tags == null ) {
            continue;
          }

          for ( String tagName : tags.keySet() ) {
            String tagVal = tags.get( tagName ).toString();
            String combined = quote( tagName ) + " : " + quote( tagVal ); //$NON-NLS-1$
            tempTags.add( combined );
          }
        }
      }
    }

    return new ArrayList<String>( tempTags );
  }

  protected static String quote( String string ) {
    if ( string.indexOf( '"' ) >= 0 ) {

      if ( string.indexOf( '"' ) >= 0 ) {
        string = string.replace( "\"", "\\\"" ); //$NON-NLS-1$ //$NON-NLS-2$
      }
    }

    string = ( "\"" + string + "\"" ); //$NON-NLS-1$ //$NON-NLS-2$

    return string;
  }

  /**
   * Return a list of replica set members whos tags satisfy the supplied list of tag set. It is assumed that members
   * satisfy according to an OR relationship = i.e. a member satisfies if it satisfies at least one of the tag sets in
   * the supplied list.
   *
   * @param tagSets the list of tag sets to match against
   * @return a list of replica set members who's tags satisfy the supplied list of tag sets
   * @throws MongoDbException if a problem occurs
   */
  public List<String> getReplicaSetMembersThatSatisfyTagSets( List<DBObject> tagSets ) throws MongoDbException {
    try {
      List<String> result = new ArrayList<String>();
      for ( DBObject object : checkForReplicaSetMembersThatSatisfyTagSets( tagSets, getRepSetMemberRecords() ) ) {
        result.add( object.toString() );
      }
      return result;
    } catch ( Exception ex ) {
      if ( ex instanceof MongoDbException ) {
        throw (MongoDbException) ex;
      } else {
        throw new MongoDbException( BaseMessages.getString( PKG,
          "MongoNoAuthWrapper.ErrorMessage.UnableToGetReplicaSetMembers" ), ex ); //$NON-NLS-1$
      }
    }
  }

  protected List<DBObject> checkForReplicaSetMembersThatSatisfyTagSets( List<DBObject> tagSets, BasicDBList members ) {
    List<DBObject> satisfy = new ArrayList<DBObject>();
    if ( members != null && members.size() > 0 ) {
      for ( Object m : members ) {
        if ( m != null ) {
          DBObject tags = (DBObject) ( (DBObject) m ).get( "tags" ); //$NON-NLS-1$
          if ( tags == null ) {
            continue;
          }

          for ( DBObject toMatch : tagSets ) {
            boolean match = true;

            for ( String tagName : toMatch.keySet() ) {
              String tagValue = toMatch.get( tagName ).toString();

              // does replica set member m's tags contain this tag?
              Object matchVal = tags.get( tagName );

              if ( matchVal == null ) {
                match = false; // doesn't match this particular tag set
                // no need to check any other keys in toMatch
                break;
              }

              if ( !matchVal.toString().equals( tagValue ) ) {
                // rep set member m's tags has this tag, but it's value does not
                // match
                match = false;

                // no need to check any other keys in toMatch
                break;
              }
            }

            if ( match ) {
              // all tag/values present and match - add this member (only if its
              // not already there)
              if ( !satisfy.contains( m ) ) {
                satisfy.add( (DBObject) m );
              }
            }
          }
        }
      }
    }

    return satisfy;
  }

  @Override
  public MongoCollectionWrapper getCollection( String db, String name ) throws MongoDbException {
    return wrap( getDb( db ).getCollection( name ) );
  }

  @Override
  public MongoCollectionWrapper createCollection( String db, String name ) throws MongoDbException {
    return wrap( getDb( db ).createCollection( name, null ) );
  }


  @Override
  public List<MongoCredential> getCredentialList() {
    // empty cred list
    return new ArrayList<MongoCredential>();
  }

  protected MongoCollectionWrapper wrap( DBCollection collection ) {
    return new DefaultMongoCollectionWrapper( collection );
  }

  @Override
  public void dispose() {
    getMongo().close();
  }

  @Override public ReplicaSetStatus getReplicaSetStatus() {
    return getMongo().getReplicaSetStatus();
  }

  @Override
  public <ReturnType> ReturnType perform( String db, MongoDBAction<ReturnType> action ) throws MongoDbException {
    return action.perform( getDb( db ) );
  }

  public MongoClientFactory getClientFactory( MongoProperties opts ) {
    return clientFactory;
  }
}
