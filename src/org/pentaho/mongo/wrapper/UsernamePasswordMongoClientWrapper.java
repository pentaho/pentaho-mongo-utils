package org.pentaho.mongo.wrapper;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.pentaho.mongo.BaseMessages;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;

public class UsernamePasswordMongoClientWrapper extends NoAuthMongoClientWrapper {
  static Class<?> PKG = UsernamePasswordMongoClientWrapper.class;

  private final String user;
  private final String password;

  /**
   * Create a connection to a Mongo server based on parameters supplied in the step meta data
   *
   * @param props properties to use
   * @param log   for logging
   * @throws MongoDbException if a problem occurs
   */
  public UsernamePasswordMongoClientWrapper( MongoProperties props, MongoUtilLogger log )
      throws MongoDbException {
    super( props, log );
    user = props.get( MongoProp.USER );
    password = props.get( MongoProp.PASSWORD );
    // TODO:   Handle decryption?
    //password = Encr.decryptPasswordOptionallyEncrypted( vars.environmentSubstitute( meta.getAuthenticationPassword
    // () ) );
  }

  public UsernamePasswordMongoClientWrapper( MongoClient mongo, MongoUtilLogger log, String user,
                                             String password ) {
    super( mongo, log );
    this.user = user;
    this.password = password;
  }

  public String getUser() {
    return user;
  }

  @Override
  protected MongoClient getClient( MongoProperties props, MongoUtilLogger log,
                                   List<ServerAddress> repSet, boolean useAllReplicaSetMembers,
                                   MongoClientOptions opts ) throws MongoDbException {
    try {
      List<MongoCredential> credList = new ArrayList<MongoCredential>();
      credList.add( getCredential( props ) );
      return ( repSet.size() > 1 || ( useAllReplicaSetMembers && repSet.size() >= 1 ) ? new MongoClient( repSet,
        credList, opts ) : ( repSet.size() == 1 ? new MongoClient( repSet.get( 0 ), credList, opts )
        : new MongoClient( new ServerAddress( "localhost" ), credList, opts ) ) ); //$NON-NLS-1$
    } catch ( UnknownHostException u ) {
      throw new MongoDbException( u );
    }
  }

  /**
   * Create a credentials object
   *
   * @param props properties to use
   * @return a configured MongoCredential object
   */
  protected MongoCredential getCredential( MongoProperties props ) {
    //    return MongoCredential.createMongoCRCredential(
    //      vars.environmentSubstitute( meta.getAuthenticationUser() ), vars
    //        .environmentSubstitute( meta.getDbName() ), Encr.decryptPasswordOptionallyEncrypted(
    //        vars.environmentSubstitute( meta.getAuthenticationPassword() ) ).toCharArray() );
    // TODO:  Handle decryption?

    return MongoCredential.createMongoCRCredential(
      props.get( MongoProp.USER ),
      props.get( MongoProp.DBNAME ),
      props.get( MongoProp.PASSWORD ).toCharArray() );
  }

  protected DB getDb( String dbName ) throws MongoDbException {
    try {
      DB result = getMongo().getDB( dbName );
      authenticateWithDb( result );
      return result;
    } catch ( Exception e ) {
      if ( e instanceof MongoDbException ) {
        throw (MongoDbException) e;
      } else {
        throw new MongoDbException( e );
      }
    }
  }

  protected void authenticateWithDb( DB db ) throws MongoDbException {
    CommandResult comResult = db.authenticateCommand( user, password.toCharArray() );
    if ( !comResult.ok() ) {
      throw new MongoDbException( BaseMessages.getString( PKG,
        "MongoUsernamePasswordWrapper.ErrorAuthenticating.Exception", //$NON-NLS-1$
        comResult.getErrorMessage() ) );
    }
  }
}
