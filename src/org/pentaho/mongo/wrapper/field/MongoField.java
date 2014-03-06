package org.pentaho.mongo.wrapper.field;

import org.pentaho.mongo.BaseMessages;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.Util;

import java.util.ArrayList;
import java.util.List;

public class MongoField implements Comparable<MongoField> {
  protected static Class<?> PKG = MongoField.class; // for i18n purposes

  /** The name the the field will take in the outputted kettle stream */
  public String m_fieldName = ""; //$NON-NLS-1$

  /** The path to the field in the Mongo object */
  public String m_fieldPath = ""; //$NON-NLS-1$

  /** The kettle type for this field */
  public String m_kettleType = ""; //$NON-NLS-1$

  /** User-defined indexed values for String types */
  public List<String> m_indexedVals;

  /**
   * Temporary variable to hold the min:max array index info for fields determined when sampling documents for
   * paths/types
   */
  public transient String m_arrayIndexInfo;

  /**
   * Temporary variable to hold the number of times this path was seen when sampling documents to determine paths/types.
   */
  public transient int m_percentageOfSample = -1;

  /**
   * Temporary variable to hold the num times this path was seen/num sampled documents. Note that numerator might be
   * larger than denominator if this path is encountered multiple times in an array within one document.
   */
  public transient String m_occurenceFraction = ""; //$NON-NLS-1$

  public transient Class<?> m_mongoType;

  /**
   * Temporary variable used to indicate that this path occurs multiple times over the sampled documents and that the
   * types differ. In this case we should default to Kettle type String as a catch-all
   */
  public transient boolean m_disparateTypes;

  /** The index that this field is in the output row structure */
  public int m_outputIndex;


  private List<String> m_pathParts;
  //private List<String> m_tempParts;

  public MongoField copy() {
    MongoField newF = new MongoField();
    newF.m_fieldName = m_fieldName;
    newF.m_fieldPath = m_fieldPath;
    newF.m_kettleType = m_kettleType;

    // reference doesn't matter here as this list is read only at runtime
    newF.m_indexedVals = m_indexedVals;

    return newF;
  }

  /**
   * Initialize this mongo field
   * 
   * @param outputIndex
   *          the index for this field in the outgoing row structure.
   * @throws MongoDbException
   *           if a problem occurs
   */
  public void init( int outputIndex ) throws MongoDbException {
    if ( Util.isEmpty( m_fieldPath ) ) {
      throw new MongoDbException( BaseMessages.getString( PKG, "MongoDbOutput.Messages.MongoField.Error.NoPathSet" ) ); //$NON-NLS-1$
    }

    if ( m_pathParts != null ) {
      return;
    }

    String fieldPath = Util.cleansePath( m_fieldPath );

    String[] temp = fieldPath.split( "\\." ); //$NON-NLS-1$
    m_pathParts = new ArrayList<String>();
    for ( String part : temp ) {
      m_pathParts.add( part );
    }

    if ( m_pathParts.get( 0 ).equals( "$" ) ) { //$NON-NLS-1$
      m_pathParts.remove( 0 ); // root record indicator
    } else if ( m_pathParts.get( 0 ).startsWith( "$[" ) ) { //$NON-NLS-1$

      // strip leading $ off of array
      String r = m_pathParts.get( 0 ).substring( 1, m_pathParts.get( 0 ).length() );
      m_pathParts.set( 0, r );
    }

//    m_tempParts = new ArrayList<String>();
    m_outputIndex = outputIndex;
  }

  /**
   * Reset this field, ready for processing a new document
   * 
   */
//  public void reset() {
//    // first clear because there may be stuff left over from processing
//    // the previous mongo document object (especially if a path exited early
//    // due to non-existent field or array index out of bounds)
//    m_tempParts.clear();
//
//    for ( String part : m_pathParts ) {
//      // m_tempParts.add( space.environmentSubstitute( part ) );
//      m_tempParts.add( part );
//    }
//  }


  @Override
  public int compareTo( MongoField comp ) {
    return m_fieldName.compareTo( comp.m_fieldName );
  }
}
