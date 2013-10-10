package org.exist.indexing.lucene;

import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.lucene.document.Field;
import org.exist.dom.QName;
import org.exist.indexing.lucene.analyzers.NoDiacriticsStandardAnalyzer;
import org.exist.storage.ElementValue;
import org.exist.storage.NodePath;
import org.exist.util.DatabaseConfigurationException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class LuceneConfigXML {

    private final static String CONFIG_ROOT = "lucene";
    private final static String INDEX_ELEMENT = "text";
    private final static String ANALYZER_ELEMENT = "analyzer";
    protected final static String FIELD_TYPE_ELEMENT = "fieldType";
    private static final String INLINE_ELEMENT = "inline";
    private static final String IGNORE_ELEMENT = "ignore";
    private final static String BOOST_ATTRIB = "boost";
    private static final String DIACRITICS = "diacritics";

    /**
     * Parse a configuration entry. The main configuration entries for this index
     * are the &lt;text&gt; elements. They may be enclosed by a &lt;lucene&gt; element.
     *
     * @param configNodes
     * @param namespaces
     * @throws org.exist.util.DatabaseConfigurationException
     */
    protected static LuceneConfig parseConfig(NodeList configNodes, Map<String, String> namespaces) throws DatabaseConfigurationException {
        LuceneConfig conf = new LuceneConfig();
        
    	Node node;
        for(int i = 0; i < configNodes.getLength(); i++) {
            node = configNodes.item(i);
            if(node.getNodeType() == Node.ELEMENT_NODE) {
                try {
					if (CONFIG_ROOT.equals(node.getLocalName())) {
					    Element elem = (Element) node;
					    if (elem.hasAttribute(BOOST_ATTRIB)) {
					        String value = elem.getAttribute(BOOST_ATTRIB);
					        try {
					        	conf.boost = Float.parseFloat(value);
					        } catch (NumberFormatException e) {
					            throw new DatabaseConfigurationException("Invalid value for 'boost' attribute in " +
					                "lucene index config: float expected, got " + value);
					        }
					    }
                        if (elem.hasAttribute(DIACRITICS)) {
                            String value = elem.getAttribute(DIACRITICS);
                            if (value.equalsIgnoreCase("no")) {
                            	conf.analyzers.setDefaultAnalyzer(new NoDiacriticsStandardAnalyzer(LuceneIndex.LUCENE_VERSION_IN_USE));
                            }
                        }
					    parseConfig(node.getChildNodes(), namespaces);
                        
					} else if (ANALYZER_ELEMENT.equals(node.getLocalName())) {
						conf.analyzers.addAnalyzer((Element) node);
                        
					} else if (FIELD_TYPE_ELEMENT.equals(node.getLocalName())) {
						FieldType type = parseFieldType((Element) node, conf.analyzers);
						conf.fieldTypes.put(type.getId(), type);
                        
					} else if (INDEX_ELEMENT.equals(node.getLocalName())) {
						// found an index definition
					    Element elem = (Element) node;
						LuceneIndexConfig config = parseLuceneIndexConfig(elem, namespaces, conf.analyzers, conf.fieldTypes);
						// if it is a named index, add it to the namedIndexes map
						if (config.getName() != null) {
							conf.namedIndexes.put(config.getName(), config);
                        }

						// register index either by QName or path
						if (config.getNodePath().hasWildcard()) {
							conf.wildcardPaths.add(config);
						} else {
						    LuceneIndexConfig idxConf = conf.paths.get(config.getNodePath().getLastComponent());
						    if (idxConf == null) {
						    	conf.paths.put(config.getNodePath().getLastComponent(), config);
                            }
						    else {
                                idxConf.add(config);
                            }
						}
                        
					} else if (INLINE_ELEMENT.equals(node.getLocalName())) {
					    Element elem = (Element) node;
					    QName qname = parseQName(elem, namespaces);
					    if (conf.inlineNodes == null) {
					    	conf.inlineNodes = new TreeSet<QName>();
                        }
					    conf.inlineNodes.add(qname);
                        
					} else if (IGNORE_ELEMENT.equals(node.getLocalName())) {
					    Element elem = (Element) node;
					    QName qname = parseQName(elem, namespaces);
					    if (conf.ignoreNodes == null) {
					    	conf.ignoreNodes = new TreeSet<QName>();
                        }
					    conf.ignoreNodes.add(qname);
					}
                    
                } catch (DatabaseConfigurationException e) {
					LuceneConfig.LOG.warn("Invalid lucene configuration element: " + e.getMessage());
				}
            }
        }
        
        return conf;
    }
    
	private final static String ID_ATTR = "id";
	private final static String ANALYZER_ID_ATTR = "analyzer";
	//private final static String BOOST_ATTRIB = "boost";
	private final static String STORE_ATTRIB = "store";
	private final static String TOKENIZED_ATTR = "tokenized";
	
    private static FieldType parseFieldType(Element config, AnalyzerConfig analyzers) throws DatabaseConfigurationException {
    	FieldType type = new FieldType();
    	
    	if (FIELD_TYPE_ELEMENT.equals(config.getLocalName())) {
    		type.id = config.getAttribute(ID_ATTR);
    		if (type.id == null || type.id.length() == 0)
    			throw new DatabaseConfigurationException("fieldType needs an attribute 'id'");
    	}
    	
    	String aId = config.getAttribute(ANALYZER_ID_ATTR);
    	// save Analyzer for later use in LuceneMatchListener
        if (aId != null && aId.length() > 0) {
        	type.analyzer = analyzers.getAnalyzerById(aId);
            if (type.analyzer == null)
                throw new DatabaseConfigurationException("No analyzer configured for id " + aId);
            type.analyzerId = aId;
            
        } else {
        	type.analyzer = analyzers.getDefaultAnalyzer();
        }
        
        String boostAttr = config.getAttribute(BOOST_ATTRIB);
        if (boostAttr != null && boostAttr.length() > 0) {
            try {
            	type.boost = Float.parseFloat(boostAttr);
            } catch (NumberFormatException e) {
                throw new DatabaseConfigurationException("Invalid value for attribute 'boost'. Expected float, " +
                        "got: " + boostAttr);
            }
        }
        
        String storeAttr = config.getAttribute(STORE_ATTRIB);
        if (storeAttr != null && storeAttr.length() > 0) {
        	type.isStore = storeAttr.equalsIgnoreCase("yes");
        	type.store = type.isStore ? Field.Store.YES : Field.Store.NO;
        }

        String tokenizedAttr = config.getAttribute(TOKENIZED_ATTR);
        if (tokenizedAttr != null && tokenizedAttr.length() > 0) {
        	type.isTokenized = tokenizedAttr.equalsIgnoreCase("yes");
        }
        
        return type;
    }
    

    private final static String QNAME_ATTR = "qname";
    private final static String MATCH_ATTR = "match";

    //private final static String IGNORE_ELEMENT = "ignore";
    //private final static String INLINE_ELEMENT = "inline";
	private final static String FIELD_ATTR = "field";
	private final static String TYPE_ATTR = "type";

	private final static String PATTERN_ATTR = "attribute";

    private static LuceneIndexConfig parseLuceneIndexConfig(
    		Element config, 
    		Map<String, String> namespaces, 
    		AnalyzerConfig analyzers,
    		Map<String, FieldType> fieldTypes
    		) throws DatabaseConfigurationException {
    	
    	LuceneIndexConfig conf = new LuceneIndexConfig();
    	
        if (config.hasAttribute(QNAME_ATTR)) {
            QName qname = parseQName(config, namespaces);
            conf.path = new NodePath(qname);
            conf.isQNameIndex = true;
        } else {
            String matchPath = config.getAttribute(MATCH_ATTR);
            try {
            	conf.path = new NodePath(namespaces, matchPath);
				if (conf.path.length() == 0)
				    throw new DatabaseConfigurationException("Lucene module: Invalid match path in collection config: " +
				        matchPath);
			} catch (IllegalArgumentException e) {
				throw new DatabaseConfigurationException("Lucene module: invalid qname in configuration: " + e.getMessage());
			}
        }

        if (config.hasAttribute(PATTERN_ATTR)) {
        	String pattern = config.getAttribute(PATTERN_ATTR);
        	int pos = pattern.indexOf("=");
        	if (pos > 0) {
        		conf.attrName = parseQName(pattern.substring(0, pos), namespaces);
        		try {
        			conf.attrValuePattern = Pattern.compile(pattern.substring(pos+1));
        		} catch (PatternSyntaxException e) {
        			throw new DatabaseConfigurationException(config.toString(), e);
        		}
        		conf.isAttrPatternIndex = true;
        	} else {
    			throw new DatabaseConfigurationException("Valid pattern 'attribute-name=pattern', but get '"+config.toString()+"'");
        	}
        } else {
        	conf.isAttrPatternIndex = false;
        }

        String name = config.getAttribute(FIELD_ATTR);
        if (name != null && name.length() > 0)
        	conf.setName(name);
        
        String fieldType = config.getAttribute(TYPE_ATTR);
        if (fieldType != null && fieldType.length() > 0)
        	conf.type = fieldTypes.get(fieldType);        
        if (conf.type == null)
        	conf.type = parseFieldType(config, analyzers);

        parse(conf, config, namespaces);
        
        return conf;
    }

    private static void parse(LuceneIndexConfig conf, Element root, Map<String, String> namespaces) throws DatabaseConfigurationException {
        Node child = root.getFirstChild();
        while (child != null) {
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                if (IGNORE_ELEMENT.equals(child.getLocalName())) {
                    String qnameAttr = ((Element) child).getAttribute(QNAME_ATTR);
                    if (qnameAttr == null || qnameAttr.length() == 0)
                        throw new DatabaseConfigurationException("Lucene configuration element 'ignore' needs an attribute 'qname'");
                    if (conf.specialNodes == null)
                    	conf.specialNodes = new TreeMap<QName, String>();
                    conf.specialNodes.put(parseQName(qnameAttr, namespaces), LuceneIndexConfig.N_IGNORE);
                } else if (INLINE_ELEMENT.equals(child.getLocalName())) {
                    String qnameAttr = ((Element) child).getAttribute(QNAME_ATTR);
                    if (qnameAttr == null || qnameAttr.length() == 0)
                        throw new DatabaseConfigurationException("Lucene configuration element 'inline' needs an attribute 'qname'");
                    if (conf.specialNodes == null)
                    	conf.specialNodes = new TreeMap<QName, String>();
                    conf.specialNodes.put(parseQName(qnameAttr, namespaces), LuceneIndexConfig.N_INLINE);
                }
            }
            child = child.getNextSibling();
        }
    }
    
    public static QName parseQName(Element config, Map<String, String> namespaces) throws DatabaseConfigurationException {
        String name = config.getAttribute(QNAME_ATTR);
        if (name == null || name.length() == 0)
            throw new DatabaseConfigurationException("Lucene index configuration error: element " + config.getNodeName() +
                    " must have an attribute " + QNAME_ATTR);

        return parseQName(name, namespaces);
    }

    protected static QName parseQName(String name, Map<String, String> namespaces) throws DatabaseConfigurationException {
        boolean isAttribute = false;
        if (name.startsWith("@")) {
            isAttribute = true;
            name = name.substring(1);
        }
        try {
            String prefix = QName.extractPrefix(name);
            String localName = QName.extractLocalName(name);
            String namespaceURI = "";
            if (prefix != null) {
                namespaceURI = namespaces.get(prefix);
                if(namespaceURI == null) {
                    throw new DatabaseConfigurationException("No namespace defined for prefix: " + prefix +
                            " in index definition");
                }
            }
            QName qname = new QName(localName, namespaceURI, prefix);
            if (isAttribute)
                qname.setNameType(ElementValue.ATTRIBUTE);
            return qname;
        } catch (IllegalArgumentException e) {
            throw new DatabaseConfigurationException("Lucene index configuration error: " + e.getMessage(), e);
        }
    }
}