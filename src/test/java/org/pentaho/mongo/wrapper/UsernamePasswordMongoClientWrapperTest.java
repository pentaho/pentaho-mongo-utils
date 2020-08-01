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

import com.mongodb.MongoCredential;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

import java.util.List;

/**
 * Test class for {@link org.pentaho.mongo.wrapper.UsernamePasswordMongoClientWrapper}.
 *
 * @author Aleksandr Kozlov
 */
public class UsernamePasswordMongoClientWrapperTest {

  /** Mocked MongoUtilLogger for UsernamePasswordMongoClientWrapper initialization. */
  @Mock private MongoUtilLogger log;

  /** Builder for MongoProperties initialization. */
  private MongoProperties.Builder mongoPropertiesBuilder;

  @Before
  public void before() {
    MockitoAnnotations.initMocks( this );
  }

  /**
   * Test of {@link UsernamePasswordMongoClientWrapper#getCredentialList()} method basic behavior.
   *
   * @throws Exception
   */
  @Test
  public void getCredentialListTest() throws Exception {
    final String username = "testuser";
    final String password = "testpass";
    final String authDb = "testuser-auth-db";
    final String dbName = "database";

    mongoPropertiesBuilder = new MongoProperties.Builder()
      .set( MongoProp.USERNAME, username )
      .set( MongoProp.PASSWORD, password )
      .set( MongoProp.AUTH_DATABASE, authDb )
      .set( MongoProp.DBNAME, dbName );
    UsernamePasswordMongoClientWrapper mongoClientWrapper =
      new UsernamePasswordMongoClientWrapper( mongoPropertiesBuilder.build(), log );
    List<MongoCredential> credentials = mongoClientWrapper.getCredentialList();
    Assert.assertEquals( 1, credentials.size() );
    Assert.assertEquals( null, credentials.get( 0 ).getMechanism() );
    Assert.assertEquals( username, credentials.get( 0 ).getUserName() );
    Assert.assertEquals( authDb, credentials.get( 0 ).getSource() );
    Assert.assertArrayEquals( password.toCharArray(), credentials.get( 0 ).getPassword() );
  }

  /**
   * Test of {@link UsernamePasswordMongoClientWrapper#getCredentialList()} method's default behavior.
   *
   * @throws Exception
   */
  @Test
  public void getCredentialListUsernameOnlyTest() throws Exception {
    final String username = "testuser";
    final String source = "dbname";
    mongoPropertiesBuilder =
      new MongoProperties.Builder().set( MongoProp.USERNAME, username ).set( MongoProp.DBNAME, source );
    UsernamePasswordMongoClientWrapper mongoClientWrapper =
        new UsernamePasswordMongoClientWrapper( mongoPropertiesBuilder.build(), log );
    List<MongoCredential> credentials = mongoClientWrapper.getCredentialList();
    Assert.assertEquals( 1, credentials.size() );
    Assert.assertEquals( null, credentials.get( 0 ).getMechanism() );
    Assert.assertEquals( username, credentials.get( 0 ).getUserName() );
    Assert.assertEquals( source, credentials.get( 0 ).getSource() );
    Assert.assertArrayEquals( "".toCharArray(), credentials.get( 0 ).getPassword() );
  }

  /**
   * Test of {@link UsernamePasswordMongoClientWrapper#getCredentialList()} method's behavior
   * when MongoProp.AUTH_DATABASE is null or empty. In this case MongoProp.AUTH_DATABASE should
   * be used for the backward compatibility.
   *
   * @throws Exception
   */
  @Test
  public void getCredentialListEmptyAuthDatabaseTest() throws Exception {
    final String username = "testuser";
    final String password = "testpass";
    final String dbName = "database";

    // MongoProp.AUTH_DATABASE is null
    mongoPropertiesBuilder = new MongoProperties.Builder()
      .set( MongoProp.USERNAME, username )
      .set( MongoProp.PASSWORD, password )
      .set( MongoProp.AUTH_DATABASE, "" )
      .set( MongoProp.DBNAME, dbName );
    UsernamePasswordMongoClientWrapper mongoClientWrapper =
      new UsernamePasswordMongoClientWrapper( mongoPropertiesBuilder.build(), log );
    List<MongoCredential> credentials = mongoClientWrapper.getCredentialList();
    Assert.assertEquals( 1, credentials.size() );
    Assert.assertEquals( null, credentials.get( 0 ).getMechanism() );
    Assert.assertEquals( username, credentials.get( 0 ).getUserName() );
    Assert.assertEquals( dbName, credentials.get( 0 ).getSource() );
    Assert.assertArrayEquals( password.toCharArray(), credentials.get( 0 ).getPassword() );

    // MongoProp.AUTH_DATABASE is empty string
    mongoPropertiesBuilder.set( MongoProp.AUTH_DATABASE, null );
    mongoClientWrapper = new UsernamePasswordMongoClientWrapper( mongoPropertiesBuilder.build(), log );
    credentials = mongoClientWrapper.getCredentialList();
    Assert.assertEquals( 1, credentials.size() );
    Assert.assertEquals( null, credentials.get( 0 ).getMechanism() );
    Assert.assertEquals( username, credentials.get( 0 ).getUserName() );
    Assert.assertEquals( dbName, credentials.get( 0 ).getSource() );
    Assert.assertArrayEquals( password.toCharArray(), credentials.get( 0 ).getPassword() );
  }

  @Test
  public void getCredentialAuthMechanism() throws Exception {
    final String username = "testuser";
    final String password = "testpass";
    final String dbName = "database";
    final String authMecha = "SCRAM-SHA-1";

    // MongoProp.AUTH_DATABASE is null
    mongoPropertiesBuilder = new MongoProperties.Builder()
      .set( MongoProp.USERNAME, username )
      .set( MongoProp.PASSWORD, password )
      .set( MongoProp.AUTH_DATABASE, "" )
      .set( MongoProp.DBNAME, dbName )
      .set( MongoProp.AUTH_MECHA, authMecha );

    UsernamePasswordMongoClientWrapper mongoClientWrapper =
      new UsernamePasswordMongoClientWrapper( mongoPropertiesBuilder.build(), log );
    List<MongoCredential> credentials = mongoClientWrapper.getCredentialList();
    Assert.assertEquals( 1, credentials.size() );
    Assert.assertEquals( authMecha, credentials.get( 0 ).getMechanism() );
    Assert.assertEquals( username, credentials.get( 0 ).getUserName() );
    Assert.assertEquals( dbName, credentials.get( 0 ).getSource() );
    Assert.assertArrayEquals( password.toCharArray(), credentials.get( 0 ).getPassword() );
  }


}