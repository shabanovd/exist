package org.exist.indexing.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;

/**
 * Configures a field type: analyzers etc. used for indexing
 * a field.
 * 
 * @author wolf
 *
 */
public class FieldType {

	protected String id = null;
	
	protected String analyzerId = null;
	
    // save Analyzer for later use in LuceneMatchListener
	protected Analyzer analyzer = null;

	protected float boost = -1;
    
	protected Field.Store store = null;
	
	protected boolean isStore = false;
	protected boolean isTokenized = false;
	
    public FieldType() {
    }
    
    public String getId() {
		return id;
	}

	public String getAnalyzerId() {
		return analyzerId;
	}

	public Analyzer getAnalyzer() {
		return analyzer;
	}

	public float getBoost() {
		return boost;
	}
	
	public Field.Store getStore() {
		return store;
	}

	public boolean isTokenized() {
		return isTokenized;
	}
	
	org.apache.lucene.document.FieldType ft = null;
	
	public org.apache.lucene.document.FieldType getFieldType() {
		if (ft == null) {
			org.apache.lucene.document.FieldType _ft = new org.apache.lucene.document.FieldType();
		
			_ft.setStored(isStore);
			_ft.setTokenized(isTokenized);
			_ft.setStoreTermVectors(true);
			_ft.setIndexed(true);
			
			ft = _ft;
		}
		
		return ft;
	}
}