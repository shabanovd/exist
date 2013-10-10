package org.exist.indexing.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.exist.dom.AttrImpl;
import org.exist.dom.QName;
import org.exist.storage.NodePath;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LuceneIndexConfig {

    protected final static String N_INLINE = "inline";
    protected final static String N_IGNORE = "ignore";

    protected String name = null;

    protected NodePath path = null;

    protected boolean isQNameIndex = false;
    protected boolean isAttrPatternIndex = false;

    protected Map<QName, String> specialNodes = null;

    protected LuceneIndexConfig nextConfig = null;
    
    protected FieldType type = null;
    
    protected QName attrName = null;
    protected Pattern attrValuePattern = null;
    
    public LuceneIndexConfig() {
    }

    // return saved Analyzer for use in LuceneMatchListener
    public Analyzer getAnalyzer() {
        return type.getAnalyzer();
    }

    public String getAnalyzerId() {
        return type.getAnalyzerId();
    }

    public QName getQName() {
        return path.getLastComponent();
    }

    public NodePath getNodePath() {
        return path;
    }

    public float getBoost() {
        return type.getBoost();
    }

    public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}
	
	public void add(LuceneIndexConfig config) {
		if (nextConfig == null)
			nextConfig = config;
		else
			nextConfig.add(config);
	}
	
	public LuceneIndexConfig getNext() {
		return nextConfig;
	}
	
	/**
	 * @return true if this index can be queried by name
	 */
	public boolean isNamed() {
		return name != null;
	}

	public boolean isIgnoredNode(QName qname) {
        return specialNodes != null && specialNodes.get(qname) == N_IGNORE;
    }

    public boolean isInlineNode(QName qname) {
        return specialNodes != null && specialNodes.get(qname) == N_INLINE;
    }
    
    public boolean isAttrPattern() {
    	return isAttrPatternIndex;
    }

    public boolean match(NodePath other) {
    	if (isQNameIndex) {
    		return other.getLastComponent().equalsSimple(path.getLastComponent());
    	}
        return path.match(other);
    }

    public boolean match(NodePath other, AttrImpl attrib) {
		if (isAttrPatternIndex) {
			if (attrib != null && attrValuePattern != null) { //log error?
				if ((isQNameIndex && other.getLastComponent().equalsSimple(path.getLastComponent())) || path.match(other)) {
					
					if (attrib.getQName().equalsSimple(attrName)) {
						
						Matcher m = attrValuePattern.matcher(attrib.getValue());
						return m.matches();
					}
					
				}
			}
		} else {
	    	if (isQNameIndex) {
	    		return other.getLastComponent().equalsSimple(path.getLastComponent());
	    	}
	        return path.match(other);
    	}
		return false;
    }


    @Override
	public String toString() {
		return path.toString();
	}
}

