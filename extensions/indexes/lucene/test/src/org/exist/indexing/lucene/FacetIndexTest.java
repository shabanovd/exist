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
package org.exist.indexing.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.exist.dom.DocumentSet;
import org.exist.dom.QName;
import org.exist.storage.DBBroker;
import org.junit.Test;

public class FacetIndexTest extends FacetAbstractTest {
	
	private final static String CREATED = "created";
	private final static String STATUS = "status";

    protected static String XUPDATE_START =
        "<xu:modifications version=\"1.0\" xmlns:xu=\"http://www.xmldb.org/xupdate\">";

    protected static String XUPDATE_END =
        "</xu:modifications>";
    
    private static String XML5 =
            "<article>" +
            "   <head>The <b>title</b>of it</head>" +
            "   <p>A simple paragraph with <hi>highlighted</hi> text <note>and a note</note> " +
            "       in it.</p>" +
            "   <p>Paragraphs with <s>mix</s><s>ed</s> content are <s>danger</s>ous.</p>" +
            "   <p><note1>ignore</note1> <s2>warn</s2>ings</p>" +
            "   <p>Another simple paragraph.</p>" +
            "</article>";

    private static String COLLECTION_CONFIG5 =
            "<collection xmlns=\"http://exist-db.org/collection-config/1.0\">" +
            "   <index xmlns:tei=\"http://www.tei-c.org/ns/1.0\">" +
            "       <fulltext default=\"none\" attributes=\"no\">" +
            "       </fulltext>" +
            "       <lucene>" +
            "           <text qname=\"article\">" +
            "               <ignore qname=\"note\"/>" +
            "               <inline qname=\"s\"/>" +
            "           </text>" +
            "           <text qname=\"p\">" +
            "               <ignore qname=\"note\"/>" +
            "               <inline qname=\"s\"/>" +
            "           </text>" +
            "           <text qname=\"head\"/>" +
            "           <ignore qname=\"note1\"/>" +
            "           <inline qname=\"s2\"/>" +
            "       </lucene>" +
            "   </index>" +
            "</collection>";

    private static Map<String, String> metas1 = new HashMap<String, String>();
    static {
        metas1.put(STATUS, "draft");
        metas1.put(CREATED, "20130803");
    }

    private static Map<String, String> metas2 = new HashMap<String, String>();
    static {
        metas2.put(STATUS, "final");
        metas2.put(CREATED, "20130805");
    }

    private static Map<String, String> metas3 = new HashMap<String, String>();
    static {
        metas3.put(STATUS, "draft");
        metas3.put(CREATED, "20130807");
    }

    private static Map<String, String> metas4 = new HashMap<String, String>();
    static {
        metas4.put(STATUS, "final");
        metas4.put(CREATED, "20130811");
    }

    private void checkFacet(List<FacetResult> facets) {
        assertEquals(1, facets.size());
        
        FacetResult facet = facets.get(0);
        assertEquals(2, facet.getNumValidDescendants());
        FacetResultNode node = facet.getFacetResultNode();
        assertEquals(0.0, node.value, 0.0001);
        assertEquals("status", node.label.toString());
        
        List<FacetResultNode> subResults = node.subResults;
        assertEquals(2, subResults.size());
        
        node = subResults.get(0);
        assertEquals(1.0, node.value, 0.0001);
        assertEquals("status/final", node.label.toString());
        
        node = subResults.get(1);
        assertEquals(1.0, node.value, 0.0001);
        assertEquals("status/draft", node.label.toString());
    }

    @Test
    public void inlineAndIgnore() {
        System.out.println("Test simple queries ...");
        DocumentSet docs = configureAndStore(COLLECTION_CONFIG5, 
                new Resource[] {
                    new Resource("test1.xml", XML5, metas1),
                    new Resource("test2.xml", XML5, metas2),
                });
        
        DBBroker broker = null;
        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());
            assertNotNull(broker);

//            checkIndex(docs, broker, new QName[] { new QName("head", "") }, "title", 2);
//            checkIndex(docs, broker, new QName[] { new QName("p", "") }, "simple", 2);
//            checkIndex(docs, broker, new QName[] { new QName("p", "") }, "mixed", 2);
//            checkIndex(docs, broker, new QName[] { new QName("p", "") }, "dangerous", 2);
//            checkIndex(docs, broker, new QName[] { new QName("p", "") }, "note", 0);
//            checkIndex(docs, broker, new QName[] { new QName("p", "") }, "ignore", 0);
//            checkIndex(docs, broker, new QName[] { new QName("p", "") }, "warnings", 2);
            
            final LuceneIndexWorker worker = (LuceneIndexWorker) broker.getIndexController().getWorkerByIndexId(LuceneIndex.ID);
            
            FacetSearchParams fsp = new FacetSearchParams(
                new CountFacetRequest(new CategoryPath(STATUS), 10)
//                new CountFacetRequest(new CategoryPath("Author"), 10)
            );
            
            CountDocuments cb = new CountDocuments();
            
            List<QName> qnames = new ArrayList<QName>();
            qnames.add(new QName("head", ""));
            List<FacetResult> results = QueryDocuments.query(worker, docs, qnames, "title", fsp, null, cb);
            
            assertEquals(2, cb.count);
            
            checkFacet(results);
            
            cb.count = 0;

            //Lucene query
            QName qname = new QName("head", "");
            
            String field = LuceneUtil.encodeQName(new QName("head", ""), db.getSymbols());
            
            Analyzer analyzer = worker.getAnalyzer(null, qname, broker, docs);

            QueryParser parser = new QueryParser(LuceneIndex.LUCENE_VERSION_IN_USE, field, analyzer);

            //worker.setOptions(options, parser);

            Query query = parser.parse("title");
            
            
            results = QueryDocuments.query(worker, docs, query, fsp, cb);
            
            assertEquals(2, cb.count);
            
            checkFacet(results);

            cb.count = 0;
            
            //check document filtering
            qnames = new ArrayList<QName>();
            qnames.add(new QName("p", ""));
            results = QueryDocuments.query(worker, docs, qnames, "paragraph", fsp, null, cb);
            
            for (FacetResult result : results) {
                System.out.println(result.toString());
            }
            
            assertEquals(2, cb.count);
            
            checkFacet(results);
            
            cb.count = 0;


//            seq = xquery.execute("/article[ft:query(p, 'highlighted')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(1, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(p, 'mixed')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(1, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(p, 'mix')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(0, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(p, 'dangerous')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(1, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(p, 'ous')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(0, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(p, 'danger')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(0, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(p, 'note')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(0, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(., 'highlighted')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(1, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(., 'mixed')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(1, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(., 'dangerous')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(1, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(., 'warnings')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(1, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(., 'danger')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(0, seq.getItemCount());
//
//            seq = xquery.execute("/article[ft:query(., 'note')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(0, seq.getItemCount());
//            
//            seq = xquery.execute("/article[ft:query(., 'ignore')]", null, AccessContext.TEST);
//            assertNotNull(seq);
//            assertEquals(0, seq.getItemCount());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            db.release(broker);
        }
    }
    
    private void checkFacet2(List<FacetResult> facets) {
        assertEquals(1, facets.size());
        
        FacetResult facet = facets.get(0);
        assertEquals(1, facet.getNumValidDescendants());
        FacetResultNode node = facet.getFacetResultNode();
        assertEquals(0.0, node.value, 0.0001);
        assertEquals("status", node.label.toString());
        
        List<FacetResultNode> subResults = node.subResults;
        assertEquals(1, subResults.size());
        
        node = subResults.get(0);
        assertEquals(2.0, node.value, 0.0001);
        assertEquals("status/draft", node.label.toString());
    }

    @Test
    public void sorting() {
        System.out.println("Test sorting queries ...");
        DocumentSet docs = configureAndStore(COLLECTION_CONFIG5, 
                new Resource[] {
                    new Resource("test1.xml", XML5, metas1),
                    new Resource("test2.xml", XML5, metas2),
                    new Resource("test3.xml", XML5, metas3),
                    new Resource("test4.xml", XML5, metas4),
                });
        
        DBBroker broker = null;
        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());
            assertNotNull(broker);

            final LuceneIndexWorker worker = (LuceneIndexWorker) broker.getIndexController().getWorkerByIndexId(LuceneIndex.ID);
            
            FacetSearchParams fsp = new FacetSearchParams(
                new CountFacetRequest(new CategoryPath(STATUS), 10)
//                new CountFacetRequest(new CategoryPath("Author"), 10)
            );
            
            Sort sort = new Sort(new SortField(CREATED, SortField.Type.STRING, true));
            
            CountDocuments cb = new CountDocuments();
            
            List<QName> qnames = worker.getDefinedIndexes(null);
            
            BooleanQuery bq = new  BooleanQuery();

            //set filter-like on meta
            bq.add(new TermQuery(new Term(STATUS, "draft")), BooleanClause.Occur.MUST);
            
            String searchText = "paragraph";
            for (QName qname : qnames) {
            	final String field = LuceneUtil.encodeQName(qname, db.getSymbols());
            	
            	//System.out.println(qname);
            	
            	bq.add(new PrefixQuery(new Term(field, searchText)), BooleanClause.Occur.SHOULD);
            }

            List<FacetResult> results = QueryDocuments.query(worker, docs, bq, fsp, cb, 5, sort);
            
            assertEquals(2, cb.count);
            
            for (FacetResult result : results) {
            	System.out.println(result);
            }
            
            checkFacet2(results);
            
            System.out.println("================");
            
            cb.count = 0;

            sort = new Sort(new SortField(CREATED, SortField.Type.STRING, false));
            
            results = QueryDocuments.query(worker, docs, bq, fsp, cb, 5, sort);
            
            assertEquals(2, cb.count);
            
            checkFacet2(results);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            db.release(broker);
        }
    }
}

