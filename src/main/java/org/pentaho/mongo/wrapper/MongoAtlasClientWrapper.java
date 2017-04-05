package org.pentaho.mongo.wrapper;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ReplicaSetStatus;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.wrapper.collection.DefaultMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by dams on 13-02-2017.
 */
public class MongoAtlasClientWrapper implements MongoClientWrapper{
  private static Class<?> PKG = MongoAtlasClientWrapper.class;

  static MongoClientFactory clientFactory = new DefaultMongoClientFactory();

  private final MongoClient mongo;
  private final MongoClientURI mongoClientURI;
  private final MongoUtilLogger log;
  protected MongoProperties props;


  MongoAtlasClientWrapper(MongoProperties props, MongoUtilLogger log) throws MongoDbException {
    this.log = log;
    this.props= props;

    String uri = new StringBuilder("mongodb://").append( props.get( MongoProp.USERNAME ) ).append( ":" ).append( props.get( MongoProp.PASSWORD )).append( "@" ).append( props.get( MongoProp.HOST ) ).toString();
    this.mongoClientURI = new MongoClientURI(uri);
    this.mongo = new MongoClient(mongoClientURI);
    String breakP ="";
  }

  MongoAtlasClientWrapper(MongoClient mongoClient, MongoClientURI mongoClientURI, MongoProperties props, MongoUtilLogger log ){
    this.mongoClientURI=mongoClientURI;
    this.mongo=mongoClient;
    this.log=log;
    this.props=props;
  }

  MongoClient getMongo(){
    return mongo;
  }

  /**
   * Retrieve all database names found in MongoDB as visible by the authenticated user.
   *
   * @throws MongoDbException
   */
  @Override
  public List<String> getDatabaseNames() throws MongoDbException {
    try {
      return  getMongo().getDatabaseNames();
    }catch (Exception e ){
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
  @Override
  public List<String> getLastErrorModes() throws MongoDbException {
    return null;
  }

  @Override
  public List<String> getIndexInfo(String s, String s1) throws MongoDbException {
    return null;
  }

  @Override
  public List<String> getAllTags() throws MongoDbException {
    return null;
  }

  /**
   * Return a list of replica set members whos tags satisfy the supplied list of tag set. It is assumed that members
   * satisfy according to an OR relationship = i.e. a member satisfies if it satisfies at least one of the tag sets in
   * the supplied list.
   *
   *
   * @return a list of replica set members who's tags satisfy the supplied list of tag sets
   * @throws MongoDbException if a problem occurs
   */
  @Override
  public List<String> getReplicaSetMembersThatSatisfyTagSets(List<DBObject> list) throws MongoDbException {
    return null;
  }

  protected MongoCollectionWrapper wrap( DBCollection collection ) {
    return new DefaultMongoCollectionWrapper( collection );
  }

  @Override
  public MongoCollectionWrapper getCollection(String db, String name ) throws MongoDbException {
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

  public MongoClientFactory getClientFactory(MongoProperties opts ) {
    return clientFactory;
  }
}
