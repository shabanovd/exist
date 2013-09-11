package org.exist.indexing.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.exist.util.DatabaseConfigurationException;
import org.w3c.dom.Element;

/**
 * Configures a field type: analyzers etc. used for indexing
 * a field.
 * 
 * @author wolf
 *
 */
public class FieldType {

	private final static String ID_ATTR = "id";
	private final static String ANALYZER_ID_ATTR = "analyzer";
	private final static String BOOST_ATTRIB = "boost";
	private final static String STORE_ATTRIB = "store";
	private final static String TOKENIZED_ATTR = "tokenized";
	
	private String id = null;
	
	private String analyzerId = null;
	
    // save Analyzer for later use in LuceneMatchListener
    private Analyzer analyzer = null;

	private float boost = -1;
    
	private Field.Store store = null;
	
	private boolean isStore = false;
	private boolean isTokenized = false;
	
    public FieldType(Element config, AnalyzerConfig analyzers) throws DatabaseConfigurationException {
        
    	if (LuceneConfig.FIELD_TYPE_ELEMENT.equals(config.getLocalName())) {
    		id = config.getAttribute(ID_ATTR);
    		if (id == null || id.length() == 0)
    			throw new DatabaseConfigurationException("fieldType needs an attribute 'id'");
    	}
    	
    	String aId = config.getAttribute(ANALYZER_ID_ATTR);
    	// save Analyzer for later use in LuceneMatchListener
        if (aId != null && aId.length() > 0) {
        	analyzer = analyzers.getAnalyzerById(aId);
            if (analyzer == null)
                throw new DatabaseConfigurationException("No analyzer configured for id " + aId);
            analyzerId = aId;
            
        } else {
        	analyzer = analyzers.getDefaultAnalyzer();
        }
        
        String boostAttr = config.getAttribute(BOOST_ATTRIB);
        if (boostAttr != null && boostAttr.length() > 0) {
            try {
                boost = Float.parseFloat(boostAttr);
            } catch (NumberFormatException e) {
                throw new DatabaseConfigurationException("Invalid value for attribute 'boost'. Expected float, " +
                        "got: " + boostAttr);
            }
        }
        
        String storeAttr = config.getAttribute(STORE_ATTRIB);
        if (storeAttr != null && storeAttr.length() > 0) {
        	isStore = storeAttr.equalsIgnoreCase("yes");
        	store = isStore ? Field.Store.YES : Field.Store.NO;
        }

        String tokenizedAttr = config.getAttribute(TOKENIZED_ATTR);
        if (tokenizedAttr != null && tokenizedAttr.length() > 0) {
        	isTokenized = tokenizedAttr.equalsIgnoreCase("yes");
        }
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