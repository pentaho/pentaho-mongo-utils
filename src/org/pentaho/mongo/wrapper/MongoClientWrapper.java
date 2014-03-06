package org.pentaho.mongo.wrapper;

import com.mongodb.DBObject;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.List;
import java.util.Set;

public interface MongoClientWrapper {
  public Set<String> getCollectionsNames( String dB ) throws MongoDbException;

  public List<String> getIndexInfo( String dbName, String collection ) throws MongoDbException;

  public List<MongoField> discoverFields( String db, String collection, String query, String fields,
      boolean isPipeline, int docsToSample ) throws MongoDbException;

  /**
   * Retrieve all database names found in MongoDB as visible by the authenticated user.
   * 
   * @throws MongoDbException
   */
  public List<String> getDatabaseNames() throws MongoDbException;

  /**
   * Get a list of all tagName : tagValue pairs that occur in the tag sets defined across the replica set.
   * 
   * @return a list of tags that occur in the replica set configuration
   * @throws MongoDbException
   *           if a problem occurs
   */
  public List<String> getAllTags() throws MongoDbException;

  /**
   * Return a list of replica set members whos tags satisfy the supplied list of tag set. It is assumed that members
   * satisfy according to an OR relationship = i.e. a member satisfies if it satisfies at least one of the tag sets in
   * the supplied list.
   * 
   * @param tagSets
   *          the list of tag sets to match against
   * @return a list of replica set members who's tags satisfy the supplied list of tag sets
   * @throws MongoDbException
   *           if a problem occurs
   */
  public List<String> getReplicaSetMembersThatSatisfyTagSets( List<DBObject> tagSets ) throws MongoDbException;

  /**
   * Return a list of custom "lastErrorModes" (if any) defined in the replica set configuration object on the server.
   * These can be used as the "w" setting for the write concern in addition to the standard "w" values of <number> or
   * "majority".
   * 
   * @return a list of the names of any custom "lastErrorModes"
   * @throws MongoDbException
   *           if a problem occurs
   */
  public List<String> getLastErrorModes() throws MongoDbException;

  public MongoCollectionWrapper createCollection( String db, String name ) throws MongoDbException;

  public MongoCollectionWrapper getCollection( String db, String name ) throws MongoDbException;

  public void dispose() throws MongoDbException;
}
