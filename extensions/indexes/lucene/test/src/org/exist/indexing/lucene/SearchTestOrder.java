/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2014 The eXist Project
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
 */
package org.exist.indexing.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.exist.collections.Collection;
import org.exist.dom.*;
import org.exist.storage.DBBroker;
import org.exist.xmldb.XmldbURI;
import org.junit.Test;

import java.util.*;

import static org.exist.indexing.lucene.LuceneIndex.LUCENE_VERSION_IN_USE;
import static org.junit.Assert.assertEquals;

public class SearchTestOrder extends FacetAbstract {

    public static final String TITLE = "title";

    private static String CONF =
            "<collection xmlns=\"http://exist-db.org/collection-config/1.0\">" +
                    "	<index>" +
                    "       <text qname=\"para\"/>" +
                    "       <text qname=\"title\"/>" +
                    "	</index>" +
                    "</collection>";

    private static Map<String, String> metas1 = new HashMap<String, String>();
    static {
        metas1.put(TITLE, "some something A");
    }

    private static Map<String, String> metas2 = new HashMap<String, String>();
    static {
        metas2.put(TITLE, "some Something let go");
    }

    private static Map<String, String> metas3 = new HashMap<String, String>();
    static {
        metas3.put(TITLE, "some one more something");
    }

    private static String XML1 =
            "<root>" +
                    "   <title>some paragraph with <hi>mixed</hi> content.</title>" +
                    "   <para>another paragraph with <note><hi>nested</hi> inner</note> elements.</para>" +
                    "   <para>a third paragraph with <term>term</term>.</para>" +
                    "   <para>double match double match</para>" +
                    "</root>";

    private static String XML2 =
            "<root>" +
                    "   <title>some another paragraph with <hi>mixed</hi> content.</title>" +
                    "   <para>another paragraph with <note><hi>nested</hi> inner</note> elements.</para>" +
                    "   <para>a third paragraph with <term>term</term>.</para>" +
                    "   <para>double match double match</para>" +
                    "</root>";

    private static String XML3 =
            "<root>" +
                    "   <title>some paragraph too with <hi>mixed</hi> content.</title>" +
                    "   <para>another paragraph with <note><hi>nested</hi> inner</note> elements.</para>" +
                    "   <para>a third paragraph with <term>term</term>.</para>" +
                    "   <para>double match double match</para>" +
                    "</root>";

    private class MyCallback implements SearchCallback<DocumentImpl> {

        @Override
        public void totalHits(Integer integer) {
            System.out.println("Total hits: " + integer);
        }

        @Override
        public void found(AtomicReader atomicReader, int i, org.exist.dom.DocumentImpl nodes, float v) {
            System.out.println("Found! uri : " + nodes.getURI().toASCIIString() + " " + v);
        }
    }

    private class NodesCallback implements SearchCallback<NodeProxy> {

        List<NodeProxy> hits = new ArrayList<NodeProxy>();

        @Override
        public void totalHits(Integer integer) {
            System.out.println("Total hits: " + integer);
        }

        @Override
        public void found(AtomicReader atomicReader, int i, NodeProxy node, float v) {
            System.out.println("Found! uri : " + node.getDocument().getURI().toASCIIString() + " " + v);

            hits.add(node);
        }
    }

    @Test
    public void runTestOrder() throws Exception {

        DBBroker broker = null;

        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());

            MutableDocumentSet docs = new DefaultDocumentSet(1031);

            String URI = "/db";
            Collection collection = broker.getCollection(XmldbURI.xmldbUriFor(URI));
            collection.allDocs(broker, docs, true);

            LuceneIndexWorker worker = (LuceneIndexWorker) broker.getIndexController().getWorkerByIndexId(LuceneIndex.ID);

            List<QName> qNames = worker.getDefinedIndexes(null);

            String[] fields = new String[qNames.size()+1];
            int i = 0;
            for (QName qName : qNames) {
                fields[i] = LuceneUtil.encodeQName(qName, db.getSymbols());
                i++;
            }
            fields[i] = "eXist:file-name"; // File name field.

            // Parse the query with no default field:
            Analyzer analyzer = new StandardAnalyzer(LUCENE_VERSION_IN_USE);

            MultiFieldQueryParser parser = new MultiFieldQueryParser(LUCENE_VERSION_IN_USE, fields, analyzer);

            Query query = parser.parse("learning and training");
            FacetSearchParams facetsConfig = null; //new FacetSearchParams(new CountFacetRequest(new CategoryPath("a"), 1));

            List<SortField> sortFields = new ArrayList<SortField>();

            sortFields.add(SortField.FIELD_SCORE);
//            SortField scoreField = SortField.FIELD_SCORE;
//            sortFields.add(
//                new SortField(
//                    scoreField.getField(), 
//                    scoreField.getType(), 
//                    true
//                )
//            );

            Sort sort = new Sort(sortFields.toArray(new SortField[sortFields.size()]));

            System.out.println("HERE STARTS");

            QueryDocuments.query(worker, docs, query, facetsConfig, new MyCallback(), 4, sort);

            System.out.println("HERE DONE");

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            db.release(broker);
        }

    }

    @Test
    public void testOrder() throws Exception {


        String[] ttt = new String[] {metas1.get(TITLE), metas2.get(TITLE), metas3.get(TITLE)};

        System.out.println(Arrays.toString(ttt));

        Arrays.sort(ttt);

        System.out.println(Arrays.toString(ttt));

        //DocumentSet docs =
        configureAndStore(CONF,
                new FacetAbstract.Resource[] {
                        new FacetAbstract.Resource("test1.xml", XML1, metas1),
                        new FacetAbstract.Resource("test2.xml", XML2, metas2),
                        new FacetAbstract.Resource("test3.xml", XML3, metas3),
                });

        DBBroker broker = null;

        try {

            broker = db.get(db.getSecurityManager().getSystemSubject());

            MutableDocumentSet docs = new DefaultDocumentSet(1031);

            String URI = "/db";
            Collection collection = broker.getCollection(XmldbURI.xmldbUriFor(URI));
            collection.allDocs(broker, docs, true);

            LuceneIndexWorker worker = (LuceneIndexWorker) broker.getIndexController().getWorkerByIndexId(LuceneIndex.ID);

            String sortField = null;

            List<QName> qNames = worker.getDefinedIndexes(null);

            String[] fields = new String[qNames.size()+1];
            int i = 0;
            for (QName qName : qNames) {
                fields[i] = LuceneUtil.encodeQName(qName, db.getSymbols());

                if (qName.getLocalName().equals(TITLE)) {
                    sortField = fields[i];

                    System.out.println(sortField);
                }

                i++;
            }
            fields[i] = TITLE;//"eXist:file-name"; // File name field.

            sortField = TITLE;
            fields = new String[] {sortField};

            // Parse the query with no default field:
            Analyzer analyzer = new StandardAnalyzer(LUCENE_VERSION_IN_USE);

            MultiFieldQueryParser parser = new MultiFieldQueryParser(LUCENE_VERSION_IN_USE, fields, analyzer);

            Query query = parser.parse("some*");
            FacetSearchParams facetsConfig = null; //new FacetSearchParams(new CountFacetRequest(new CategoryPath("a"), 1));

            SortField[] sortFields = new SortField[1];

            //sortFields.add(new SortField(TITLE, SortField.Type.STRING));
            sortFields[0] = new SortField(sortField, SortField.Type.STRING);

            Sort sort = new Sort(sortFields);

            System.out.println("HERE STARTS");

            NodesCallback cb = new NodesCallback();

            QueryNodes.query(worker, null, -1, docs, query, facetsConfig, cb, 10000, sort);

            System.out.println("2 3 1");

            assertEquals(cb.hits.size(), 12);

            String[] expected = {"/db/test/test2.xml",
                    "/db/test/test2.xml",
                    "/db/test/test2.xml",
                    "/db/test/test2.xml",

                    "/db/test/test3.xml",
                    "/db/test/test3.xml",
                    "/db/test/test3.xml",
                    "/db/test/test3.xml",

                    "/db/test/test1.xml",
                    "/db/test/test1.xml",
                    "/db/test/test1.xml",
                    "/db/test/test1.xml"
            };

            i = 0;
            for (NodeProxy node : cb.hits) {
                assertEquals(node.getDocument().getDocumentURI().toString(), expected[i++]);
            }

            System.out.println("HERE DONE");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.release(broker);
        }

    }

//    @BeforeClass
//    public static void startDB() throws Exception {
//        File confFile = ConfigurationHelper.lookup("conf.xml");
//        Configuration config = new Configuration(confFile.getAbsolutePath());
//        BrokerPool.configure(1, 5, config);
//    }
//
//    @AfterClass
//    public static void closeDB() {
//        //TestUtils.cleanupDB();
//        BrokerPool.stopAll(false);
//    }

}