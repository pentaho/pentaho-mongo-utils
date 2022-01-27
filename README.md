pentaho-mongo-utils
===================

A library for simplifying the creation and management of MongoDB connections.


Example
=======

Create new MongoClientWrappers via the factory method below, passing in the properties required.

````java
MongoClientWrapper mongo = MongoClientWrapperFactory.createMongoClientWrapper(
      new MongoProperties.Builder()
        .set( MongoProp.HOST, "localhost" )
        .set( MongoProp.PORT, "27017" )
        .set( MongoProp.USER, "user" )
        .set( MongoProp.PASSWORD, "password" )
        .set( MongoProp.DBNAME, "databaseName" ).build(),
      null );
MongoCollectionWrapper collection  = 
    mongo.getCollection( "databaseName", "collectionName" );
MongoCursorWrapper cursor = collection.find();
      
````

MongoProperties
===============

For the most part, the expected values of MongoProperties are consistent with their corresponding properties in the MongoClientUri specification (http://api.mongodb.org/java/2.12/com/mongodb/MongoClientURI.html).  Some exceptions include:

* JOURNALED:  a true|false property indicating how WriteConcern should be configured
* KERBEROS:  a true|false property indicating whether the GSSAPI auth mechanism should be used
* TAG_SET:  A comma seperated, ordered list of JSON docs defining the tag sets to be used for configuring readPreference.  For example:  { "disk": "ssd", "use": "reporting", "rack": "a" },{ "disk": "ssd", "use": "reporting", "rack": "d" }

See org.pentaho.mongo.MongoProp for the full set of configuration properties.


Authentication
==============

The factory method will instantiate a MongoClientWrapper that is appropriate for the requested authentication context.  Currently supported mechanisms includes:

* GSSAPI (Kerberos):  Kerberos connections will be attempted if the KERBEROS property is set to "true".  All interaction with MongoDB will be performed under the authentication context of the KERBEROS principal.
* Plain:  Clear-text user/pass.
* NoAuth:  fallback if the USER, PASSWORD, and KERBEROS properties are all unset.

Setting up a dev environment for Kerberos
=========================================

The test suite of this project runs a series of tests using Kerberos authentication, both with cached credentials
as well as a keytab file.  To setup your environment to run these tests:

1)  Install the kerberos client (for debian linux:  apt-get install krb5-user)   
2)  Configure your /etc/krb5.conf file with the kerberos server's information.  E.g.
````
    [realms]
     PENTAHO.QA = {
      kdc = bad-badkdc-cent.pentaho.qa
      admin_server = bad-badkdc-cent.pentaho.qa
     }

    [domain_realm]
     .pentaho.qa = PENTAHO.QA
     pentaho.qa = PENTAHO.QA
````
3.  Retrieve Kerberos credentials by running "kinit <principalName>" (e.g. "kinit buildguy@PENTAHO.QA").  This will prompt for a password,
    and assuming authentication succeeds, will generate credentials.  You can verify credentials were actually
    created by running "klist", which should show something like this:
````
Ticket cache: FILE:/tmp/krb5cc_1000
Default principal: buildguy@PENTAHO.QA

Valid starting    Expires           Service principal
23/03/2014 06:06  24/03/2014 06:06  krbtgt/PENTAHO.QA@PENTAHO.QA
	renew until 23/03/2014 06:06

````
4)  Run the "ktutil" command.  This will bring up a ktutil command shell.  From within ktutil, run the following
(replacing <principal> with the username):
````
ktutil:  addent -password -p <principal> -k 1 -e rc4-hmac\r
````
This will prompt for a password.  After entering it, run the command
````
ktutil:  wkt  kerberos.keytab
`````
That should write out a file in cwd with the name "kerberos.keytab".   

5)  Install JCE.  This is required for using Kerberos with AES256.
http://www.oracle.com/technetwork/java/javase/downloads/jce-7-download-432124.html  
hint: on Ubuntu if you are using the webupd8 ppa for Java you can install oracle-java8-unlimited-jce-policy
6)  If authenticating with kerberos in biserver, make sure to set the USE_KERBEROS property to true in your olap4j.properties file.  If using a keytab file, also specify PENTAHO_JAAS_AUTH_MODE=KERBEROS_KEYTAB and PENTAHO_JAAS_KEYTAB_FILE=<path/to/keytabfile>.  See MongoProps for more detail.


How to build
--------------

pentaho-mongo-utils uses the maven framework. 


#### Pre-requisites for building the project:
* Maven, version 3+
* Java JDK 11
* This [settings.xml](https://raw.githubusercontent.com/pentaho/maven-parent-poms/master/maven-support-files/settings.xml) in your <user-home>/.m2 directory

#### Building it

This is a maven project, and to build it use the following command

```
$ mvn clean install
```
Optionally you can specify -Drelease to trigger obfuscation and/or uglification (as needed)

Optionally you can specify -Dmaven.test.skip=true to skip the tests (even though
you shouldn't as you know)

The build result will be a Pentaho package located in ```target```.

#### Running the tests

__Unit tests__

This will run all unit tests in the project (and sub-modules). To run integration tests as well, see Integration Tests below.

```
$ mvn test
```

If you want to remote debug a single java unit test (default port is 5005):

```
$ cd core
$ mvn test -Dtest=<<YourTest>> -Dmaven.surefire.debug
```

Running Integration Tests
=========================================

In addition to the unit tests, there are integration tests in the core project.
```
$ mvn verify -DrunITs
```
The integration tests will connect to MongoDB servers using plain authentication, kerberos authentication, and SSL.  The kerberos tests require generating a keytab file, which is done during the pre-integration-test phase of the mvn build.  The kerberos user and password need to be specified on the command line as follows:

````
mvn -DrunITs -Dkerberos.principal=user@domain -Dkerberos.password=password install
````

Troubleshooting Kerberos
========================

Kerberos can be tricky to get working.  Pentaho infocenter has good information on initial setup:  http://infocenter.pentaho.com/help/index.jsp?topic=%2Fpdi_admin_guide%2Ftask_kerberos_mongodb.html

Adding the java property sun.security.krb5.debug=true provides some debug level logging to standard out.  If AES256 encryption is being used, a common error is the following, which indicates the JVM does not have JCE available.  

__IntelliJ__

* Don't use IntelliJ's built-in maven. Make it use the same one you use from the commandline.
  * Project Preferences -> Build, Execution, Deployment -> Build Tools -> Maven ==> Maven home directory

````



