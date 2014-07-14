package org.exist.indexing.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.dom.DefaultDocumentSet;
import org.exist.dom.DocumentImpl;
import org.exist.dom.MutableDocumentSet;
import org.exist.dom.QName;
import org.exist.indexing.lucene.*;
import org.exist.security.xacml.AccessContext;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.util.Configuration;
import org.exist.util.ConfigurationHelper;
import org.exist.util.DatabaseConfigurationException;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.TerminatedException;
import org.exist.xquery.XQueryContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Matt Hallman
 * Date: 3/28/14
 * Time: 4:10 PM
 */
public class SearchTestsFacets {



    public SearchTestsFacets() {
    }

    private class MyCallback implements SearchCallback<DocumentImpl> {
        private String previous = "";

        @Override
        public void totalHits(Integer integer) {
            System.out.println("Total hits: " + integer);
        }

        @Override
        public void found(AtomicReader atomicReader, int i, org.exist.dom.DocumentImpl nodes, float v) {

            System.out.println(" Doc uri: " + nodes.getURI());

//            String current = nodes.getFileURI().toString();
//            int compare = previous.compareTo(current);
//            previous = current;

//            System.out.println("Compare: " + compare + " Doc uri: " + nodes.getFileURI());



            // title sorting
//            try {
//                FileEntity file = fielsAPI.getEntity(new FileByURILocator(nodes.getURI().toASCIIString()));
//                int compare = previous.compareTo(((XMLDocImpl) file).getTitle());
//                previous = ((XMLDocImpl) file).getTitle();
//                System.out.println("Compare: " + compare + " Doc uri: " + nodes.getFileURI());
//            } catch (APIException e) {
//                e.printStackTrace();
//            }


//            System.out.println("Found! uri : " + nodes.getURI().toASCIIString() + " " + v);
        }
    }

    /**
     * Tests facets.
     */
    public void runTestFacets() throws EXistException {

        BrokerPool db = BrokerPool.getInstance();

        try (DBBroker broker = db.get(db.getSecurityManager().getSystemSubject())) {

            MutableDocumentSet docs = new DefaultDocumentSet(1031);

            String URI = "/db/organizations/test-org/repositories/";
            Collection collection = broker.getCollection(XmldbURI.xmldbUriFor(URI)); //TODO hardcoded
            collection.allDocs(broker, docs, true);

            XQueryContext context = new XQueryContext(broker.getDatabase(), AccessContext.XMLDB);

            LuceneIndexWorker worker = (LuceneIndexWorker) context.getBroker().getIndexController().getWorkerByIndexId(LuceneIndex.ID);

            List<QName> qNames = new ArrayList<QName>();
            qNames = worker.getDefinedIndexes(qNames);

            // Parse the query with no default field:
            Analyzer analyzer = new StandardAnalyzer(LuceneIndex.LUCENE_VERSION_IN_USE);

            String[] fields = new String[qNames.size()+1];
            int i = 0;
            for (QName qName : qNames) {
                fields[i] = LuceneUtil.encodeQName(qName, db.getSymbols());
                i++;
            }
            fields[i] = "eXist:file-name"; // File name field.

            MultiFieldQueryParser parser = new MultiFieldQueryParser(LuceneIndex.LUCENE_VERSION_IN_USE, fields, analyzer);

            Query query = parser.parse("learning and training");
            FacetRequest[] facetRequests = new FacetRequest[1];

//            facetRequests[0] = new CountFacetRequest(new CategoryPath("status"), 10);
//            facetRequests[1] = new CountFacetRequest(new CategoryPath("content-type"), 10);

            //facetRequests[0] = new CountFacetRequest(new CategoryPath("status"), 10);
            facetRequests[0] = new CountFacetRequest(new CategoryPath("content-type"), 10);
            //facetRequests[0] = new CountFacetRequest(new CategoryPath("eXist:"), 10);

            FacetSearchParams fsp = new FacetSearchParams(facetRequests);

            SortField scoreField = SortField.FIELD_SCORE;

            List<FacetResult> facetResults = QueryDocuments.query(worker, docs, query, fsp, new MyCallback(), 1000, new org.apache.lucene.search.Sort(scoreField));
            for (FacetResult facetResult: facetResults) {
                System.out.println(facetResult);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

//    /**
//     * Tests search sorting.
//     */
//    public void testSorting() {
//        try {
//            DBBroker broker = null;
//            BrokerPool db = null;
//            db = BrokerPool.getInstance();
//            broker = db.get(db.getSecurityManager().getSystemSubject());
//
//            MutableDocumentSet docs = new DefaultDocumentSet(1031);
//
//            String URI = "/db/organizations/test-org/repositories/project-two/documents/sortingTestExample";
//            Collection collection = broker.getCollection(XmldbURI.xmldbUriFor(URI)); //TODO hardcoded
//            collection.allDocs(broker, docs, true);
//
//            XQueryContext context = new XQueryContext(broker.getDatabase(), AccessContext.XMLDB);
//
//            LuceneIndexWorker worker = (LuceneIndexWorker) context.getBroker().getIndexController().getWorkerByIndexId(LuceneIndex.ID);
//
//            List<QName> qNames = new ArrayList<QName>();
//            qNames = worker.getDefinedIndexes(qNames);
//
//            Query query = new MatchAllDocsQuery();
//
//            FacetRequest[] facetRequests = new FacetRequest[1];
//            facetRequests[0] = new CountFacetRequest(new CategoryPath("status"), 10);
//            FacetSearchParams fsp = new FacetSearchParams(facetRequests);
//
//            SortField sortField = new SortField("eXist:file-name", SortField.Type.STRING);
//
//            List<FacetResult> facetResults = QueryDocuments.query(worker, docs, query, fsp, new MyCallback(), 1000, new org.apache.lucene.search.Sort(sortField));
//
//        } catch (TerminatedException e) {
//            e.printStackTrace();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (EXistException e) {
//            e.printStackTrace();
//        }  catch (Exception e) {
//            e.printStackTrace();
//        } catch (Throwable t) {
//            t.printStackTrace();
//        }
//    }

    public static void main(String[] args) throws Exception {
        File confFile = ConfigurationHelper.lookup("conf.xml");
        Configuration config = new Configuration(confFile.getAbsolutePath());
        BrokerPool.configure(1, 5, config);

        SearchTestsFacets test = new SearchTestsFacets();

        test.runTestFacets();

        BrokerPool.stopAll(false);
    }

}