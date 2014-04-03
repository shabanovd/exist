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
import org.exist.dom.DefaultDocumentSet;
import org.exist.dom.DocumentImpl;
import org.exist.dom.MutableDocumentSet;
import org.exist.dom.QName;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.util.Configuration;
import org.exist.util.ConfigurationHelper;
import org.exist.xmldb.XmldbURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SearchTestOrder {


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

    //@Test
    public void runTestOrder() throws Exception {

        BrokerPool db = BrokerPool.getInstance();

        DBBroker broker = null;
        
        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());

            MutableDocumentSet docs = new DefaultDocumentSet(1031);

            String URI = "/db/organizations/test-org/repositories/";
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
            Analyzer analyzer = new StandardAnalyzer(LuceneIndex.LUCENE_VERSION_IN_USE);

            MultiFieldQueryParser parser = new MultiFieldQueryParser(LuceneIndex.LUCENE_VERSION_IN_USE, fields, analyzer);

            Query query = parser.parse("learning and training");
            FacetSearchParams fsp = null; //new FacetSearchParams(new CountFacetRequest(new CategoryPath("a"), 1));

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

            QueryDocuments.query(worker, docs, query, fsp, new MyCallback(), 4, sort);
            
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            db.release(broker);
        }

    }

    @BeforeClass
    public static void startDB() throws Exception {
        File confFile = ConfigurationHelper.lookup("conf.xml");
        Configuration config = new Configuration(confFile.getAbsolutePath());
        BrokerPool.configure(1, 5, config);
    }

    @AfterClass
    public static void closeDB() {
        //TestUtils.cleanupDB();
        BrokerPool.stopAll(false);
    }

}