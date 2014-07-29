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

import java.util.ArrayList;
import java.util.Collection;

import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.TaggableReadPreference;

public enum NamedReadPreference {

  PRIMARY( ReadPreference.primary() ),
  PRIMARY_PREFERRED( ReadPreference.primaryPreferred() ),
  SECONDARY( ReadPreference.secondary() ),
  SECONDARY_PREFERRED( ReadPreference.secondaryPreferred() ),
  NEAREST( ReadPreference.nearest() );

  private ReadPreference pref = null;

  NamedReadPreference( ReadPreference pref ) {
    this.pref = pref;
  }

  public String getName() {
    return pref.getName();
  }

  public ReadPreference getPreference() {
    return pref;
  }

  public static Collection<String> getPreferenceNames() {
    ArrayList<String> prefs = new ArrayList<String>();

    for ( NamedReadPreference preference : NamedReadPreference.values() ) {
      prefs.add( preference.getName() );
    }

    return prefs;
  }

  public ReadPreference getTaggableReadPreference( DBObject firstTagSet, DBObject... remainingTagSets ) {

    switch( this ) {
      case PRIMARY_PREFERRED:
        return ReadPreference.primaryPreferred( firstTagSet, remainingTagSets );
      case SECONDARY:
        return ReadPreference.secondary( firstTagSet, remainingTagSets );
      case SECONDARY_PREFERRED:
        return ReadPreference.secondaryPreferred( firstTagSet, remainingTagSets );
      case NEAREST:
        return ReadPreference.nearest( firstTagSet, remainingTagSets );
      default:
        return ( pref instanceof TaggableReadPreference ) ? pref : null;
    }
  }

  public static NamedReadPreference byName( String preferenceName ) {
    NamedReadPreference foundPreference = null;

    for ( NamedReadPreference preference : NamedReadPreference.values() ) {
      if ( preference.getName().equalsIgnoreCase( preferenceName ) ) {
        foundPreference = preference;
        break;
      }
    }
    return foundPreference;
  }

}
