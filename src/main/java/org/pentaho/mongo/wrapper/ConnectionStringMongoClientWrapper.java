/*!
 * Copyright 2021 Hitachi Vantara.  All rights reserved.
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

import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBList;
import com.mongodb.MongoCredential;
import com.mongodb.ReplicaSetStatus;
import org.pentaho.mongo.BaseMessages;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.Util;
import org.pentaho.mongo.wrapper.collection.DefaultMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of MongoClientWrapper which uses Connection String to get Mongo Client
 *
 *
 * .
 * Should only be instantiated by MongoClientWrapperFactory.
 */

public class ConnectionStringMongoClientWrapper implements MongoClientWrapper {
  static MongoClientFactory clientFactory = new DefaultMongoClientFactory();
  private final MongoClient mongo;
  private final MongoUtilLogger log;
  private final String connectionString;
  private static Class<?> PKG = ConnectionStringMongoClientWrapper.class;
  public static final String LOCAL_DB = "local"; //$NON-NLS-1$
  public static final String REPL_SET_COLLECTION = "system.replset"; //$NON-NLS-1$
  public static final String REPL_SET_SETTINGS = "settings"; //$NON-NLS-1$
  public static final String REPL_SET_LAST_ERROR_MODES = "getLastErrorModes"; //$NON-NLS-1$
  public static final String REPL_SET_MEMBERS = "members"; //$NON-NLS-1$
  /**
   * Create a connection to a Mongo server based on parameters supplied in the step meta data
   *
   * @param connectionString connectionString to use
   * @param log   for logging
   * @throws MongoDbException if a problem occurs
   */
  ConnectionStringMongoClientWrapper( String connectionString, MongoUtilLogger log )
          throws MongoDbException {
    this.connectionString = connectionString;
    this.log = log;
    mongo = getClientFactory( connectionString ).getConnectionStringMongoClient( connectionString );
  }
  public MongoClientFactory getClientFactory( String connectionString ) {
    return clientFactory;
  }

  MongoClient getMongo() {
    return mongo;
  }

  @Override
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

  @Override
  public List<String> getIndexInfo( String dbName, String collection ) throws MongoDbException {
    try {
      DB db = getDb( dbName );
      if ( db == null ) {
        throw new MongoDbException(
          BaseMessages.getString( PKG, "MongoConnectionStringWrapper.ErrorMessage.NonExistentDB", dbName ) ); //$NON-NLS-1$
      }
      if ( Util.isEmpty( collection ) ) {
        throw new MongoDbException(
                BaseMessages.getString( PKG, "MongoConnectionStringWrapper.ErrorMessage.NoCollectionSpecified" ) ); //$NON-NLS-1$
      }
      if ( !db.collectionExists( collection ) ) {
        db.createCollection( collection, null );
      }
      DBCollection coll = db.getCollection( collection );
      if ( coll == null ) {
        throw new MongoDbException( BaseMessages.getString( PKG,
        "MongoConnectionStringWrapper.ErrorMessage.UnableToGetInfoForCollection", //$NON-NLS-1$
         collection ) );
      }
      List<DBObject> collInfo = coll.getIndexInfo();
      List<String> result = new ArrayList<>();
      if ( collInfo == null || collInfo.isEmpty() ) {
        throw new MongoDbException( BaseMessages.getString( PKG,
                      "MongoConnectionStringWrapper.ErrorMessage.UnableToGetInfoForCollection", //$NON-NLS-1$
                      collection ) );
      }
      for ( DBObject index : collInfo ) {
        result.add( index.toString() );
      }
      return result;
    }   catch ( Exception e ) {
      log.error( BaseMessages.getString( PKG, "MongoConnectionStringWrapper.ErrorMessage.GeneralError.Message" ) //$NON-NLS-1$
          + ":\n\n" + e.getMessage(), e ); //$NON-NLS-1$
      if ( e instanceof MongoDbException ) {
        throw (MongoDbException) e;
      } else {
        throw new MongoDbException( e );
      }
    }
  }

  @Override
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

  @Override
  public List<String> getAllTags() throws MongoDbException {
    return setupAllTags( getRepSetMemberRecords() );
  }
  protected List<String> setupAllTags( BasicDBList members ) {
    HashSet<String> tempTags = new HashSet<>();
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

  private void logInfo( String message ) {
    if ( log != null ) {
      log.info( message );
    }
  }
  protected static String quote( String string ) {
    if ( string.indexOf( '"' ) >= 0 ) {
      string = string.replace( "\"", "\\\"" ); //$NON-NLS-1$ //$NON-NLS-2$
    }
    string = ( "\"" + string + "\"" ); //$NON-NLS-1$ //$NON-NLS-2$
    return string;
  }
  private BasicDBList getRepSetMemberRecords() throws MongoDbException {
    String noReplicaSetMembersDefined = BaseMessages.getString( PKG,
              "MongoConnectionStringWrapper.Message.Warning.NoReplicaSetMembersDefined" );
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
                logInfo( noReplicaSetMembersDefined ); //$NON-NLS-1$
              } else {
                setMembers = (BasicDBList) members;
              }

            } else {
              // log that there are no replica set members defined
              logInfo( noReplicaSetMembersDefined ); //$NON-NLS-1$
            }
          } else {
            // log that there are no replica set members defined
            logInfo( noReplicaSetMembersDefined ); //$NON-NLS-1$
          }
        } else {
          // log that the replica set collection is not available
          logInfo( BaseMessages.getString( PKG,
            "MongoConnectionStringWrapper.Message.Warning.ReplicaSetCollectionUnavailable" ) ); //$NON-NLS-1$
        }
      } else {
      // log that the local database is not available!!
        logInfo(
          BaseMessages.getString( PKG, "MongoConnectionStringWrapper.Message.Warning.LocalDBNotAvailable" ) ); //$NON-NLS-1$
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
  /**
   * Return a list of replica set members whose tags satisfy the supplied list of tag set. It is assumed that members
   * satisfy according to an OR relationship = i.e. a member satisfies if it satisfies at least one of the tag sets in
   * the supplied list.
   *
   * @param tagSets the list of tag sets to match against
   * @return a list of replica set members who's tags satisfy the supplied list of tag sets
   * @throws MongoDbException if a problem occurs
   */
  @Override
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
                      "MongoConnectionStringWrapper.ErrorMessage.UnableToGetReplicaSetMembers" ), ex ); //$NON-NLS-1$
      }
    }
  }
  protected List<DBObject> checkForReplicaSetMembersThatSatisfyTagSets( List<DBObject> tagSets, BasicDBList members ) {
    List<DBObject> satisfy = new ArrayList<>();
    if ( members != null && !members.isEmpty() ) {
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

              if ( matchVal == null || !matchVal.toString().equals( tagValue ) ) {
                // rep set member m's tags has this tag, but it's value does not
                // match
                match = false; // doesn't match this particular tag set
                // no need to check any other keys in toMatch
                break;
              }
            }

            if ( match && !satisfy.contains( m ) ) {
              // all tag/values present and match - add this member (only if its
              // not already there)
              satisfy.add( (DBObject) m );
            }
          }
        }
      }
    }
    return satisfy;
  }
  /**
   * Return a list of custom "lastErrorModes" (if any) defined in the replica set configuration object on the server.
   * These can be used as the "w" setting for the write concern in addition to the standard "w" values of <number> or
   * "majority".
   *
   * @return a list of the names of any custom "lastErrorModes"
   * @throws MongoDbException if a problem occurs
   */
  @Override
  public List<String> getLastErrorModes() throws MongoDbException {
    List<String> customLastErrorModes = new ArrayList<>();

    DB local = getDb( LOCAL_DB );
    if ( local != null ) {
      try {
        DBCollection replset = local.getCollection( REPL_SET_COLLECTION );
        DBObject config = replset.findOne();
        extractLastErrorModes( config, customLastErrorModes );
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

  @Override
  public List<MongoCredential> getCredentialList() {
    return getMongo().getCredentialsList();
  }
  protected MongoCollectionWrapper wrap( DBCollection collection ) {
    return new DefaultMongoCollectionWrapper( collection );
  }
  @Override
  public MongoCollectionWrapper createCollection( String db, String name ) throws MongoDbException {
    return wrap( getDb( db ).createCollection( name, null ) );
  }

  @Override
  public MongoCollectionWrapper getCollection( String db, String name ) throws MongoDbException {
    return wrap( getDb( db ).getCollection( name ) );
  }

  @Override
  public void dispose() throws MongoDbException {
    getMongo().close();
  }

  @Override
  public <ReturnType> ReturnType perform( String db, MongoDBAction<ReturnType> action ) throws MongoDbException {
    return action.perform( getDb( db ) );
  }

  @Override
  public ReplicaSetStatus getReplicaSetStatus() {
    return getMongo().getReplicaSetStatus();
  }
}
