package org.pentaho.mongo;


public interface MongoUtilLogger {

  void debug( java.lang.String s );

  void info( java.lang.String s, java.lang.Object... objects );

  void error( java.lang.String s, java.lang.Throwable throwable );

  boolean isDebugEnabled();

}
