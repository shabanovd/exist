/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2013 The eXist Project
 *  http://exist-db.org
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 *  $Id$
 */
package org.exist.xquery.modules.lucene;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

import javax.xml.transform.OutputKeys;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.Query;
import org.exist.Database;
import org.exist.collections.Collection;
import org.exist.dom.DefaultDocumentSet;
import org.exist.dom.DocumentImpl;
import org.exist.dom.MutableDocumentSet;
import org.exist.dom.NodeProxy;
import org.exist.dom.QName;
import org.exist.indexing.IndexController;
import org.exist.indexing.lucene.*;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;
import org.exist.storage.DBBroker;
import org.exist.storage.serializers.Serializer;
import org.exist.util.serializer.SAXSerializer;
import org.exist.util.serializer.SerializerPool;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.Dependency;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.NodeValue;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Class implementing the ft:search-facet() method
 * 
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class FacetSearch extends BasicFunction {

    private static final Logger LOG = Logger.getLogger(FacetSearch.class);
    
    private static final QName SEARCH = new QName("facet-search", LuceneModule.NAMESPACE_URI, LuceneModule.PREFIX);
    private static final QName SEARCH_WITH_GROUPING = new QName("facet-search-with-grouping", LuceneModule.NAMESPACE_URI, LuceneModule.PREFIX);
    private static final QName SEARCH_WITH_EXPLANATION = new QName("facet-search-with-explanation", LuceneModule.NAMESPACE_URI, LuceneModule.PREFIX);

    private static final String DESCRIPTION = "Faceted search for data at lucene index";
    private static final String DESCRIPTION_ = "Faceted search for data at lucene index (result include explanation)";

    private static final FunctionParameterSequenceType PATH =
            new FunctionParameterSequenceType("path", Type.STRING, Cardinality.ZERO_OR_MORE,
                "URI paths of documents or collections in database. Collection URIs should end on a '/'.");


    private static final FunctionParameterSequenceType QUERY = 
    		new FunctionParameterSequenceType("query", Type.ITEM, Cardinality.EXACTLY_ONE, "query string or query xml");
    
    private static final FunctionParameterSequenceType MAXHITS = 
    		new FunctionParameterSequenceType("max-hits", Type.INTEGER, Cardinality.EXACTLY_ONE, "max hits");

    private static final FunctionParameterSequenceType FACET = 
    		new FunctionParameterSequenceType("facet-request", Type.NODE, Cardinality.ZERO_OR_MORE, "facet request");

    private static final FunctionParameterSequenceType SORT = 
    		new FunctionParameterSequenceType("sort-criteria", Type.NODE, Cardinality.ZERO_OR_MORE, "sort criteria");

    private static final FunctionParameterSequenceType HIGHLIGHT = 
    		new FunctionParameterSequenceType("highlight", Type.BOOLEAN, Cardinality.EXACTLY_ONE, "highlight matcher(s)");

    private static final FunctionReturnSequenceType RETURN = 
    		new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, "All documents that are match by the query");

    /**
     * Function signatures
     */
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
    		SEARCH,
            DESCRIPTION,
            new SequenceType[]{
                PATH,
                QUERY,
                MAXHITS,
                HIGHLIGHT,
                FACET,
                SORT
            },
            RETURN
        ),
		new FunctionSignature(
			SEARCH,
			DESCRIPTION,
			new SequenceType[]{
				QUERY,
				MAXHITS,
				HIGHLIGHT,
				FACET,
				SORT
			},
			RETURN
		),

        new FunctionSignature(
            SEARCH_WITH_GROUPING,
            DESCRIPTION,
            new SequenceType[]{
                PATH,
                QUERY,
                MAXHITS,
                HIGHLIGHT,
                FACET,
                SORT
            },
            RETURN
        ),
        new FunctionSignature(
            SEARCH_WITH_GROUPING,
            DESCRIPTION,
            new SequenceType[]{
                QUERY,
                MAXHITS,
                HIGHLIGHT,
                FACET,
                SORT
            },
            RETURN
        ),

        new FunctionSignature(
            SEARCH_WITH_EXPLANATION,
            DESCRIPTION_,
            new SequenceType[]{
                PATH,
                QUERY,
                MAXHITS,
                HIGHLIGHT,
                FACET,
                SORT
            },
            RETURN
        )
    };

    /**
     * Constructor
     */
    public FacetSearch(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        NodeImpl report = null;
        try {
            // Only match documents that match these URLs 
            List<String> toBeMatchedURIs = new ArrayList<String>();

            Sequence pathSeq = getSignature() == signatures[1] ? contextSequence : args[0];
            if (pathSeq == null)
                return Sequence.EMPTY_SEQUENCE;

            // Get first agument, these are the documents / collections to search in
            for (SequenceIterator i = pathSeq.iterate(); i.hasNext(); ) {
                String path;
                Item item = i.nextItem();
                if (Type.subTypeOf(item.getType(), Type.NODE)) {
                    if (((NodeValue) item).isPersistentSet()) {
                        path = ((NodeProxy) item).getDocument().getURI().toString();
                    } else {
                        path = item.getStringValue();
                    }
                } else {
                    path = item.getStringValue();
                }
                toBeMatchedURIs.add(path);
            }

            // Get second argument, this is the query
            Item queryItem = args[getSignature() == signatures[1] ? 0 : 1].itemAt(0);

            DBBroker broker = context.getBroker();
            IndexController indexController = broker.getIndexController();

            // Get the lucene worker
            LuceneIndexWorker indexWorker =
                    (LuceneIndexWorker) indexController.getWorkerByIndexId(LuceneIndex.ID);

            LuceneIndex index = indexWorker.index;

            Query query;

            Properties options = new Properties();

            // Get analyzer : to be retrieved from configuration
            Analyzer analyzer = new StandardAnalyzer(LuceneIndex.LUCENE_VERSION_IN_USE);

            if (Type.subTypeOf(queryItem.getType(), Type.ELEMENT)) {
                XMLToQuery queryTranslator = new XMLToQuery(index);
                query = queryTranslator.parse((Element)queryItem, analyzer, options);

            } else {

                QueryParser parser = new QueryParser(LuceneIndex.LUCENE_VERSION_IN_USE, "", analyzer);
                query = parser.parse(queryItem.getStringValue());
            }
            
            int maxHits = args[getSignature() == signatures[1] ? 1 : 2].toJavaObject(int.class);

            boolean highlight = args[getSignature() == signatures[1] ? 2 : 3].effectiveBooleanValue();

            FacetSearchParams facetRequests = parseFacetRequests(args[getSignature() == signatures[1] ? 3 : 4]);
            SortParams sortParams = parseSortCriteria(args[getSignature() == signatures[1] ? 4 : 5]);

            boolean explain = getName().equals(SEARCH_WITH_EXPLANATION);

            boolean grouping = getName().equals(SEARCH_WITH_GROUPING);

            // Perform search
            report = search(broker, index, indexWorker, toBeMatchedURIs, query, maxHits, highlight, facetRequests, sortParams, grouping, explain);


        } catch (XPathException ex) {
            // Log and rethrow
            LOG.error(ex.getMessage(), ex);
            throw ex;
        } catch (ParseException e) {
            LOG.error(e.getMessage(), e);
            throw new XPathException(this, e);
        }

        // Return list of matching files.
        return report;
    }
    
    public int getDependencies() {
    	return Dependency.CONTEXT_SET;
    }
    
    private NodeImpl search(final DBBroker broker, final LuceneIndex index, final LuceneIndexWorker indexWorker,
                            final List<String> toBeMatchedURIs, final Query query, int maxHits, boolean highlight,
                            FacetSearchParams facetRequests, SortParams sortParams, boolean grouping, final boolean explain) throws XPathException {

        MutableDocumentSet docs = new DefaultDocumentSet(1031);
        try {
            for (String uri : toBeMatchedURIs) {
                XmldbURI url = XmldbURI.xmldbUriFor(uri);
                Collection col = broker.getCollection(url);
                if (col != null) {
                    col.allDocs(broker, docs, true);
                } else {
                    col = broker.getCollection(url.removeLastSegment());

                    if (col != null) {
                        DocumentImpl doc = col.getDocument(broker, url.lastSegment());
                        if (doc != null)
                            docs.add(doc);
                    }
                }
            }
        } catch (Exception ex) {
            //ex.printStackTrace();
            LuceneIndexWorker.LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex);
        }

        NodeImpl report = null;
        
        IndexSearcher searcher = null;
        try {
            // Get index searcher
            searcher = index.getSearcher();
            
            // Get analyzer : to be retrieved from configuration
//            final Analyzer searchAnalyzer = new StandardAnalyzer(LuceneIndex.LUCENE_VERSION_IN_USE);

            final MemTreeBuilder builder = new MemTreeBuilder();
            builder.startDocument();
            
            // start root element
            final int nodeNr = builder.startElement("", "results", "results", null);
            
            builder.namespaceNode("exist", "http://exist.sourceforge.net/NS/exist");
            
            QName bq = null;
            
            List<FacetResult> results = null;
            if (highlight || grouping) {

                SearchCallback<NodeProxy> cb;
                if (grouping) {
                    cb = new NodeSearchCallback(sortParams.sortByScore);
                } else {

                    cb = new SearchCallback<NodeProxy>() {

                        int total = -1;

                        @Override
                        public void totalHits(Integer number) {
                            total = number;
                        }

                        @Override
                        public void found(AtomicReader reader, int docNum, NodeProxy element, float score) {
                            if (LuceneIndex.DEBUG)
                                try {
                                    System.out.println("\n" + element.getDocument().getURI());
                                    System.out.println(queryResult2String(broker, element, 5, LuceneMatchChunkListener.DO_NOT_CHUNK_NODE));
                                } catch (Throwable e) {
                                    e.printStackTrace();
                                }

                            String fDocUri = element.getDocument().getURI().toString();

                            // setup attributes
                            AttributesImpl attribs = new AttributesImpl();
                            attribs.addAttribute("", "uri", "uri", "CDATA", fDocUri);
                            attribs.addAttribute("", "score", "score", "CDATA", "" + score);

                            // write element and attributes
                            builder.startElement("", "search", "search", attribs);
                            builder.addReferenceNode(element);
                            builder.endElement();

                            // clean attributes
                            //attribs.clear();

                            if (explain) explain(index, query, docNum, fDocUri);
                        }
                    };
                }

                // Perform actual search
                if (sortParams.luceneSort == null) {
                    results = QueryNodes.query(indexWorker, bq, getContextId(), docs, query, facetRequests, cb, maxHits);
                } else {
                    results = QueryNodes.query(indexWorker, bq, getContextId(), docs, query, facetRequests, cb, maxHits, sortParams.luceneSort);
                }

                if (cb instanceof NodeSearchCallback) {
                    ((NodeSearchCallback)cb).serialize(builder, highlight);
                }

            } else {
	            SearchCallback<DocumentImpl> cb = new SearchCallback<DocumentImpl>() {

	            	int total = -1;

					@Override
					public void totalHits(Integer number) {
						total = number;
					}

					@Override
					public void found(AtomicReader reader, int docNum, DocumentImpl document, float score) {

						String fDocUri = document.getURI().toString();

	                    // setup attributes
	                    AttributesImpl attribs = new AttributesImpl();
	                    attribs.addAttribute("", "uri", "uri", "CDATA", fDocUri);
	                    attribs.addAttribute("", "score", "score", "CDATA", ""+score);

	                    // write element and attributes
	                    builder.startElement("", "search", "search", attribs);
	                    builder.endElement();

	                    // clean attributes
	                    attribs.clear();

                        if (explain)  explain(index, query, docNum, fDocUri);
					}
				};
				
	            // Perform actual search
                if (sortParams.luceneSort == null) {
                    results = QueryDocuments.query(indexWorker, docs, query, facetRequests, cb, maxHits);
                } else {
                    results = QueryDocuments.query(indexWorker, docs, query, facetRequests, cb, maxHits, sortParams.luceneSort);
                }
            }

            if (results != null) {
	            AttributesImpl attribs = new AttributesImpl();
	            builder.startElement("", "facet-result", "facet-result", attribs);
	            //process facet results
	            for (FacetResult facet : results) {
	            	
	                //assertEquals(2, facet.getNumValidDescendants());
	                FacetResultNode root = facet.getFacetResultNode();
	                
	                
	                attribs.clear();
	                
	                attribs.addAttribute("", "label", "label", "CDATA", root.label.toString());
	                attribs.addAttribute("", "value", "value", "CDATA", String.valueOf(root.value));
	                
	                builder.startElement("", "node", "node", attribs);
	                
	                for (FacetResultNode node : root.subResults) {
	
	                    attribs.clear();
	                    
	                    attribs.addAttribute("", "label", "label", "CDATA", node.label.toString());
	                    attribs.addAttribute("", "value", "value", "CDATA", String.valueOf(node.value));
	                    
	                    builder.startElement("", "node", "node", attribs);
	
	                    builder.endElement();
	                }
	                
	                builder.endElement();
	            }
	            // finish facet element
	            builder.endElement();
            }
            
            // finish root element
            builder.endElement();
            
            //System.out.println(builder.getDocument().toString());
            
            report = builder.getDocument().getNode(nodeNr);


        } catch (Exception ex){
            //ex.printStackTrace();
            LuceneIndexWorker.LOG.error(ex.getMessage(), ex);
            throw new XPathException(this, ex);
        
        } finally {
            index.releaseSearcher(searcher);
        }
        
        
        return report;
    }

    private void explain(LuceneIndex index, org.apache.lucene.search.Query query, int doc, String url) {
        IndexSearcher searcher = null;
        try {
            searcher = index.getSearcher();
            Explanation explanation = searcher.explain(query, doc);

            System.out.println(url);
            System.out.println(explanation);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            index.releaseSearcher(searcher);
        }
    }
    
    protected FacetSearchParams parseFacetRequests(Sequence optSeq) throws XPathException {

        if (optSeq.isEmpty()) 
            return null;
//        if (optSeq.isEmpty()) 
//        	throw new XPathException(this, "Facet request can't be ZERO");
        
        if (optSeq.getItemCount() > 1) 
        	throw new XPathException(this, "Facet request can't be MANY");

        List<FacetRequest> facetRequests = new ArrayList<FacetRequest>();
        
        SequenceIterator iter = optSeq.iterate();
        while (iter.hasNext()) {
        	Element element = (Element) iter.nextItem();

        	String localName = element.getLocalName();
        	if ("count".equalsIgnoreCase(localName)) {
        		
        		String str = null;
        		int num;
        		try {
        			str = element.getAttribute("num");
        			num = Integer.valueOf( str );
        		} catch (NumberFormatException e) {
                    throw new XPathException(this, "Can't convert to integer '" + str + "'.");
        		}
        	
        		facetRequests.add(new CountFacetRequest(new CategoryPath(((Item)element).getStringValue()), num));
        	} else 
                throw new XPathException(this, "Unknown facet request '" + localName + "'.");
        }

        return new FacetSearchParams(facetRequests);
    }

    protected SortParams parseSortCriteria(Sequence optSeq) throws XPathException {

        SortParams params = new SortParams();

        if (optSeq.isEmpty())
        	return params;

        List<SortField> sortFields = new ArrayList<SortField>();
        
        SequenceIterator iter = optSeq.iterate();
        while (iter.hasNext()) {
        	Element element = (Element) iter.nextItem();

    		SortField.Type type = SortField.Type.STRING;
    		boolean reverse = false;
    		
    		String str = element.getAttribute("type");
    		if (str == null || str.isEmpty()) {
    		} else if ("STRING".equalsIgnoreCase(str)) {
        		type = SortField.Type.STRING;
    		} else {
    			throw new XPathException(this, "Unknown sort field type '"+str+"'.");
    		}
    		
    		str = element.getAttribute("reverse");
    		if (str != null && !str.isEmpty()) {
    			reverse = Boolean.valueOf(str);
    		}

    		String field = ((Item)element).getStringValue();
    		
    		if (field == null || field.isEmpty())
    			throw new XPathException(this, "Field name can't be empty or undefined.");
    		
    		if ("__score__".equalsIgnoreCase(field)) {

                params.sortByScore = true;
    		
    		    SortField scoreField = SortField.FIELD_SCORE;
                    //sortFields.add(SortField.FIELD_SCORE);
                    sortFields.add(
                        new SortField(
                            scoreField.getField(), 
                            scoreField.getType(), 
                            reverse
                        )
                    );
    		    
    		} else {
    		    sortFields.add(new SortField(field, type, reverse));
    		}
        }

        if (sortFields.size() == 1 && params.sortByScore) {

        } else {
            params.sortByScore = false;

            params.luceneSort = new Sort(sortFields.toArray(new SortField[sortFields.size()]));
        }

        return params;
    }

    static Properties props = new Properties();
    static {
        props.setProperty(OutputKeys.INDENT, "yes");
    }
 
    private String queryResult2String(DBBroker broker, NodeProxy proxy, int chunkOffset, byte mode) throws SAXException, XPathException {

        Serializer serializer = broker.getSerializer();
        serializer.reset();
        
        LuceneMatchChunkListener highlighter = new LuceneMatchChunkListener(getLuceneIndex(broker), chunkOffset, mode);
        highlighter.reset(broker, proxy);
        
        final StringWriter writer = new StringWriter();
        
        SerializerPool serializerPool = SerializerPool.getInstance();
        SAXSerializer xmlout = (SAXSerializer) serializerPool.borrowObject(SAXSerializer.class);
        try {
        	//setup pipes
			xmlout.setOutput(writer, props);
			
			highlighter.setNextInChain(xmlout);
			
			serializer.setReceiver(highlighter);
			
			//serialize
	        serializer.toReceiver(proxy, false, true);
	        
	        //get result
	        return writer.toString();
        } finally {
        	serializerPool.returnObject(xmlout);
        }
    }
    
    private LuceneIndex getLuceneIndex(DBBroker broker) {
        return (LuceneIndex) broker.getDatabase().getIndexManager().getIndexById(LuceneIndex.ID);
    }

    private class SortParams {
        Sort luceneSort = null;
        boolean sortByScore = false;
    }

    private class NodeSearchCallback implements SearchCallback<NodeProxy> {

        class EntryHits implements Comparable<EntryHits> {

            DocumentImpl doc;

            float score;

            List<NodeProxy> hits;

            EntryHits(DocumentImpl doc) {

                this.doc = doc;

                hits = new ArrayList(2);
            }

            public void addHit(NodeProxy hit, float score) {

                hits.add(hit);

                this.score += score;
            }

            @Override
            public int compareTo(EntryHits o) {
                return Float.compare( score, o.score );
            }

            public float getRank() {
                return score;
            }
        }

        boolean sortByScore = false;

        int count = 0;
        int total = 0;

        Map<DocumentImpl, EntryHits> entries = new LinkedHashMap(500);

        public NodeSearchCallback(boolean sortByScore) {
            this.sortByScore = sortByScore;
        }

        @Override
        public void totalHits(Integer integer) {
        }

        @Override
        public void found(AtomicReader r, int i, NodeProxy node, float score) {
            DocumentImpl key = node.getDocument();

            //System.out.println(key.getURI());

            EntryHits entry = entries.get(key);
            if (entry == null) {
                entry = new EntryHits(key);

                entries.put(key, entry);

                count++;
            }

            entry.addHit(node, score);

            total++;
        }

        public void serialize(MemTreeBuilder builder, boolean showHitNode) {
            List<EntryHits> finalEntries = new ArrayList(entries.size());

            for (Map.Entry<DocumentImpl, EntryHits> entry : entries.entrySet()) {
                finalEntries.add(entry.getValue());
            }

            if (sortByScore) {
                Collections.sort(finalEntries, new Comparator<EntryHits>() {
                    @Override
                    public int compare(EntryHits o1, EntryHits o2) {
                        return Float.compare(o2.getRank(), o1.getRank());
                    }
                });
            }

            for (EntryHits entry : finalEntries) {

                String fDocUri = entry.doc.getURI().toString();

                // setup attributes
                AttributesImpl attrs = new AttributesImpl();
                attrs.addAttribute("", "uri", "uri", "CDATA", fDocUri);
                attrs.addAttribute("", "score", "score", "CDATA", String.valueOf(entry.score));

                // write element and attributes
                builder.startElement("", "search", "search", attrs);

                if (showHitNode) {
                    for (NodeProxy hit : entry.hits) {
                        builder.startElement("", "hit", "hit", null);
                        builder.addReferenceNode(hit);
                        builder.endElement();
                    }
                }

                builder.endElement();
            }
        }


        public List<EntryHits> getHits(int start, int end) {

            List<EntryHits> hits = new ArrayList(entries.size());

            for (Map.Entry<DocumentImpl, EntryHits> entry : entries.entrySet()) {
                hits.add(entry.getValue());
            }

            if (sortByScore) {
                Collections.sort(hits, new Comparator<EntryHits>() {
                    @Override
                    public int compare(EntryHits o1, EntryHits o2) {
                        return Float.compare(o1.getRank(), o2.getRank());
                    }
                });
            }

            if (entries.size() < end)
                end = entries.size();

            return hits.subList(start, end);
        }
    }
}
