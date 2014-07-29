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

package org.pentaho.mongo;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

import java.util.ArrayList;
import java.util.Arrays;

import static org.pentaho.mongo.BaseMessages.getString;


class MongoPropToOption {
  private MongoUtilLogger log;

  MongoPropToOption( MongoUtilLogger log ) {
    this.log = log;
  }

  public int intValue( String value, int defaultVal ) {
    if ( !Util.isEmpty( value ) ) {
      try {
        return Integer.parseInt( value );
      } catch ( NumberFormatException n ) {
        logWarn(
          getString( PKG, "MongoPropToOption.Warning.Message.NumberFormat", value, Integer.toString( defaultVal ) ) );
        return defaultVal;
      }
    }
    return defaultVal;
  }

  public long longValue( String value, long defaultVal ) {
    if ( !Util.isEmpty( value ) ) {
      try {
        return Long.parseLong( value );
      } catch ( NumberFormatException n ) {
        logWarn(
          getString( PKG, "MongoPropToOption.Warning.Message.NumberFormat", value, Long.toString( defaultVal ) ) );
        return defaultVal;
      }
    }
    return defaultVal;
  }

  public boolean boolValue( String value, boolean defaultVal ) {
    if ( !Util.isEmpty( value ) ) {
      return Boolean.parseBoolean( value );
    }
    return defaultVal;
  }

  private static Class<?> PKG = MongoPropToOption.class;

  public ReadPreference readPrefValue( MongoProperties props ) throws MongoDbException {
    String readPreference = props.get( MongoProp.readPreference );
    if ( Util.isEmpty( readPreference ) ) {
      // nothing to do
      return null;
    }
    DBObject[] tagSets = getTagSets( props );
    NamedReadPreference preference = NamedReadPreference.byName( readPreference );
    if ( preference == null ) {
      throw new MongoDbException(
        getString( PKG, "MongoPropToOption.ErrorMessage.ReadPreferenceNotFound", readPreference,
          getPrettyListOfValidPreferences() ) );
    }
    logInfo( getString(
      PKG, "MongoPropToOption.Message.UsingReadPreference", preference.getName() ) );

    if ( preference == NamedReadPreference.PRIMARY && tagSets.length > 0 ) {
      // Invalid combination.  Tag sets are not used with PRIMARY
      logWarn( getString(
        PKG, "MongoPropToOption.Message.Warning.PrimaryReadPrefWithTagSets" ) );
      return preference.getPreference();
    } else if ( tagSets.length > 0 ) {
      logInfo(
        getString(
          PKG, "MongoPropToOption.Message.UsingReadPreferenceTagSets",
          Arrays.toString( tagSets ) ) );
      DBObject[] remainder = tagSets.length > 1 ? Arrays.copyOfRange( tagSets, 1, tagSets.length ) : new DBObject[ 0 ];
      return preference.getTaggableReadPreference( tagSets[0], remainder );
    } else {
      logInfo( getString( PKG, "MongoPropToOption.Message.NoReadPreferenceTagSetsDefined" ) );
      return preference.getPreference();
    }
  }

  private String getPrettyListOfValidPreferences() {
    // [primary, primaryPreferred, secondary, secondaryPreferred, nearest]
    return Arrays.toString( new ArrayList<String>( NamedReadPreference.getPreferenceNames() ).toArray() );
  }

  DBObject[] getTagSets( MongoProperties props ) throws MongoDbException {
    String tagSet = props.get( MongoProp.tagSet );
    if ( tagSet != null ) {
      BasicDBList list;
      if ( !tagSet.trim().startsWith( "[" ) ) {
        // wrap the set in an array
        tagSet = "[" + tagSet + "]";
      }
      try {
        list = (BasicDBList) JSON.parse( tagSet );
      } catch ( JSONParseException parseException ) {
        throw new MongoDbException(
          getString( PKG, "MongoPropToOption.ErrorMessage.UnableToParseTagSets", tagSet ),
          parseException );
      }
      return list.toArray( new DBObject[list.size()] );
    }
    return new DBObject[0];
  }

  public WriteConcern writeConcernValue( final MongoProperties props )
    throws MongoDbException {
    // write concern
    String writeConcern = props.get( MongoProp.writeConcern );
    String wTimeout = props.get( MongoProp.wTimeout );
    boolean journaled = Boolean.valueOf( props.get( MongoProp.JOURNALED ) );

    WriteConcern concern;

    if ( !Util.isEmpty( writeConcern ) && Util.isEmpty( wTimeout ) && !journaled ) {
      // all defaults - timeout 0, journal = false, w = 1
      concern = new WriteConcern();
      concern.setWObject( 1 );

      if ( log != null ) {
        log.info(
          getString( PKG, "MongoPropToOption.Message.ConfiguringWithDefaultWriteConcern" ) ); //$NON-NLS-1$
      }
    } else {
      int wt = 0;
      if ( !Util.isEmpty( wTimeout ) ) {
        try {
          wt = Integer.parseInt( wTimeout );
        } catch ( NumberFormatException n ) {
          throw new MongoDbException( n );
        }
      }

      if ( !Util.isEmpty( writeConcern ) ) {
        // try parsing as a number first
        try {
          int wc = Integer.parseInt( writeConcern );
          concern = new WriteConcern( wc, wt, false, journaled );
        } catch ( NumberFormatException n ) {
          // assume its a valid string - e.g. "majority" or a custom
          // getLastError label associated with a tag set
          concern = new WriteConcern( writeConcern, wt, false, journaled );
        }
      } else {
        concern = new WriteConcern( 1, wt, false, journaled );
      }

      if ( log != null ) {
        String lwc =
          "w = " + concern.getW() + ", wTimeout = " + concern.getWtimeout() + ", journaled = " + concern.getJ();
        log.info( getString( PKG, "MongoPropToOption.Message.ConfiguringWithWriteConcern", lwc ) );
      }
    }
    return concern;
  }

  private void logInfo( String message ) {
    if ( log != null ) {
      log.info( message );
    }
  }

  private void logWarn( String message ) {
    if ( log != null ) {
      log.warn( message, null );
    }
  }
}
