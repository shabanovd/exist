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
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.exist.collections.Collection;
import org.exist.dom.*;
import org.exist.dom.persistent.DefaultDocumentSet;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.dom.persistent.MutableDocumentSet;
import org.exist.dom.persistent.NodeProxy;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.util.Configuration;
import org.exist.util.ConfigurationHelper;
import org.exist.xmldb.XmldbURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
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

    private static Map<String, String> metas1 = new HashMap<>();
    static {
        metas1.put(TITLE, "some something A");
    }

    private static Map<String, String> metas2 = new HashMap<>();
    static {
        metas2.put(TITLE, "some Something let go");
    }

    private static Map<String, String> metas3 = new HashMap<>();
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
        public void found(AtomicReader atomicReader, int i, DocumentImpl nodes, float v) {
            System.out.println("Found! uri : " + nodes.getURI().toASCIIString() + " " + v);
        }
    }

    private class NodesCallback implements SearchCallback<NodeProxy> {

        List<NodeProxy> hits = new ArrayList<>();

        @Override
        public void totalHits(Integer integer) {
            System.out.println("Total hits: " + integer);
        }

        @Override
        public void found(AtomicReader atomicReader, int i, NodeProxy node, float v) {
            System.out.println("Found! uri : " + node.getOwnerDocument().getURI().toASCIIString() + " " + v);

            hits.add(node);
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
            }
        );

        try (DBBroker broker = db.get(db.getSecurityManager().getSystemSubject())) {

            MutableDocumentSet docs = new DefaultDocumentSet(1031);

            String URI = "/db";
            Collection collection = broker.getCollection(XmldbURI.xmldbUriFor(URI));
            collection.allDocs(broker, docs, true);

            LuceneIndexWorker worker = (LuceneIndexWorker) broker.getIndexController().getWorkerByIndexId(LuceneIndex.ID);

            List<QName> qNames = worker.getDefinedIndexes(null);

            System.out.println(Arrays.toString(qNames.toArray()));

            String[] fields = new String[qNames.size()];//+1];
            int i = 0;
            for (QName qName : qNames) {
                fields[i] = LuceneUtil.encodeQName(qName, db.getSymbols());
                i++;
            }

            Query query;

            // Parse the query with no default field:
            Analyzer analyzer = new StandardAnalyzer(LUCENE_VERSION_IN_USE);

            MultiFieldQueryParser parser = new MultiFieldQueryParser(LUCENE_VERSION_IN_USE, fields, analyzer);
            query = parser.parse("some*");
//            query = new MatchAllDocsQuery();

            FacetsConfig facetsConfig = null; //new FacetSearchParams(new CountFacetRequest(new CategoryPath("a"), 1));

            Sort sort = new Sort(new SortField(TITLE+"_sort", SortField.Type.STRING));

            System.out.println("HERE STARTS");

            NodesCallback cb = new NodesCallback();

            QueryNodes.query(worker, null, -1, docs, query, facetsConfig, cb, 10000, sort);

            System.out.println("3 1 2");
//            System.out.println("2 3 1");

            assertEquals(3, cb.hits.size());

            String[] expected = {
                "/db/test/test3.xml",
                "/db/test/test1.xml",
                "/db/test/test2.xml"
            };

            i = 0;
            for (NodeProxy node : cb.hits) {
                assertEquals(expected[i++], node.getOwnerDocument().getDocumentURI());
            }

            System.out.println("HERE DONE");

            //test reverse
            sort = new Sort(new SortField(TITLE+"_sort", SortField.Type.STRING, true));

            cb = new NodesCallback();

            QueryNodes.query(worker, null, -1, docs, query, facetsConfig, cb, 10000, sort);

            System.out.println("2 1 3");

            assertEquals(3, cb.hits.size());

            expected = new String[] {
                "/db/test/test2.xml",
                "/db/test/test1.xml",
                "/db/test/test3.xml"
            };

            i = 0;
            for (NodeProxy node : cb.hits) {
                assertEquals(expected[i++], node.getOwnerDocument().getDocumentURI());
            }

        } catch (Exception e) {
            e.printStackTrace();
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