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

package org.pentaho.mongo.wrapper;

import com.mongodb.DBObject;
import com.mongodb.MongoCredential;
import com.mongodb.ReplicaSetStatus;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import java.util.List;
import java.util.Set;

/**
 * Defines the wrapper interface for all interactions with a MongoClient.
 * This interface for the most part passes on method calls to the underlying
 * MongoClient implementations, but run in the desired AuthContext.
 * This interface also includes some convenience methods (e.g. getAllTags(),
 * getLastErrorModes()) which are not present in MongoClient.
 */
public interface MongoClientWrapper {
  public Set<String> getCollectionsNames( String dB ) throws MongoDbException;

  public List<String> getIndexInfo( String dbName, String collection ) throws MongoDbException;

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

  /**
   * Gets the list of credentials that this client authenticates all connections with.
   */
  public List<MongoCredential> getCredentialList();

  /**
   * Creates a new collection using the specified db and name
   * @param db The database name
   * @param name The new collection name
   * @return a MongoCollectionWrapper which wraps the DBCollection object.
   * @throws MongoDbException
   */
  public MongoCollectionWrapper createCollection( String db, String name ) throws MongoDbException;

  /**
   * Gets a collection with a given name. If the collection does not exist, a new collection is created.
   * @param db database name
   * @param name  collection name
   * @return a MongoCollectionWrapper which wraps the DBCollection object
   * @throws MongoDbException
   */
  public MongoCollectionWrapper getCollection( String db, String name ) throws MongoDbException;

  /**
   * Calls the close() method on the underling MongoClient.
   * @throws MongoDbException
   */
  public void dispose() throws MongoDbException;

  /**
   * @return the ReplicaSetStatus for the cluster.
   */
  ReplicaSetStatus getReplicaSetStatus();
}
