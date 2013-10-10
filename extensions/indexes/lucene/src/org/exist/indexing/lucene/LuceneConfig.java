package org.exist.indexing.lucene;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.exist.dom.QName;
import org.exist.storage.NodePath;

public class LuceneConfig {

	protected final static Logger LOG = Logger.getLogger(LuceneConfig.class);
	
	protected Map<QName, LuceneIndexConfig> paths = new TreeMap<QName, LuceneIndexConfig>();
	protected List<LuceneIndexConfig> wildcardPaths = new ArrayList<LuceneIndexConfig>();
	protected Map<String, LuceneIndexConfig> namedIndexes = new TreeMap<String, LuceneIndexConfig>();
    
	protected Map<String, FieldType> fieldTypes = new HashMap<String, FieldType>();
    
	protected Set<QName> inlineNodes = null;
	protected Set<QName> ignoreNodes = null;

    private PathIterator iterator = new PathIterator();
    
    protected float boost = -1;

    protected AnalyzerConfig analyzers = new AnalyzerConfig();

    public LuceneConfig() {
    }

    /**
     * Copy constructor. LuceneConfig is only configured once by database instance,
     * so to avoid concurrency issues when using e.g. iterator, we create a copy.
     * 
     * @param other
     */
    public LuceneConfig(LuceneConfig other) {
    	this.paths = other.paths;
    	this.wildcardPaths = other.wildcardPaths;
    	this.namedIndexes = other.namedIndexes;
    	this.fieldTypes = other.fieldTypes;
    	this.inlineNodes = other.inlineNodes;
    	this.ignoreNodes = other.ignoreNodes;
    	this.boost = other.boost;
    	this.analyzers = other.analyzers;
    }
    
    public boolean matches(NodePath path) {
        LuceneIndexConfig idxConf = paths.get(path.getLastComponent());
        while (idxConf != null) {
            if (idxConf.match(path))
                return true;
            idxConf = idxConf.getNext();
        }
        for (LuceneIndexConfig config : wildcardPaths) {
            if (config.match(path))
                return true;
        }
        return false;
    }

    public Iterator<LuceneIndexConfig> getConfig(NodePath path) {
        iterator.reset(path);
        return iterator;
    }

    protected LuceneIndexConfig getWildcardConfig(NodePath path) {
        LuceneIndexConfig config;
        for (int i = 0; i < wildcardPaths.size(); i++) {
            config = wildcardPaths.get(i);
            if (config.match(path))
                return config;
        }
        return null;
    }

    public Analyzer getAnalyzer(QName qname) {
        LuceneIndexConfig idxConf = paths.get(qname);
        while (idxConf != null) {
            if (!idxConf.isNamed() && idxConf.getNodePath().match(qname))
                break;
            idxConf = idxConf.getNext();
        }
        if (idxConf != null) {
            String id = idxConf.getAnalyzerId();
            if (id != null)
                return analyzers.getAnalyzerById(idxConf.getAnalyzerId());
        }
        return analyzers.getDefaultAnalyzer();
    }

    public Analyzer getAnalyzer(NodePath nodePath) {
        if (nodePath.length() == 0)
            throw new RuntimeException();
        LuceneIndexConfig idxConf = paths.get(nodePath.getLastComponent());
        while (idxConf != null) {
            if (!idxConf.isNamed() && idxConf.match(nodePath))
                break;
            idxConf = idxConf.getNext();
        }
        if (idxConf == null) {
            for (LuceneIndexConfig config : wildcardPaths) {
                if (config.match(nodePath))
                    return config.getAnalyzer();
            }
        }
        if (idxConf != null) {
            String id = idxConf.getAnalyzerId();
            if (id != null)
                return analyzers.getAnalyzerById(idxConf.getAnalyzerId());
        }
        return analyzers.getDefaultAnalyzer();
    }

    public Analyzer getAnalyzer(String field) {
        LuceneIndexConfig config = namedIndexes.get(field);
        if (config != null) {
            String id = config.getAnalyzerId();
            if (id != null)
                return analyzers.getAnalyzerById(config.getAnalyzerId());
        }
        return analyzers.getDefaultAnalyzer();
    }

    public Analyzer getAnalyzerById(String id) {
    	return analyzers.getAnalyzerById(id);
    }
    
    public boolean isInlineNode(QName qname) {
        return inlineNodes != null && inlineNodes.contains(qname);
    }

    public boolean isIgnoredNode(QName qname) {
        return ignoreNodes != null && ignoreNodes.contains(qname);
    }

    public float getBoost() {
        return boost;
    }
    
    public FieldType getFieldType(String name){
        return fieldTypes.get(name);
    }

    private class PathIterator implements Iterator<LuceneIndexConfig> {

        private LuceneIndexConfig nextConfig;
        private NodePath path;
        private boolean atLast = false;

        protected void reset(NodePath path) {
            this.atLast = false;
            this.path = path;
            nextConfig = paths.get(path.getLastComponent());
            if (nextConfig == null) {
                nextConfig = getWildcardConfig(path);
                atLast = true;
            }
        }

        @Override
        public boolean hasNext() {
            return (nextConfig != null);
        }

        @Override
        public LuceneIndexConfig next() {
            if (nextConfig == null)
                return null;

            LuceneIndexConfig currentConfig = nextConfig;
            nextConfig = nextConfig.getNext();
            if (nextConfig == null && !atLast) {
                nextConfig = getWildcardConfig(path);
                atLast = true;
            }
            return currentConfig;
        }

        @Override
        public void remove() {
            //Nothing to do
        }

    }
}
