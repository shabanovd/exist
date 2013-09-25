package org.exist.storage.md;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;

import org.exist.Indexer;
import org.exist.TestUtils;
import org.exist.collections.Collection;
import org.exist.collections.CollectionConfigurationManager;
import org.exist.collections.IndexInfo;
import org.exist.dom.DefaultDocumentSet;
import org.exist.dom.DocumentSet;
import org.exist.dom.MutableDocumentSet;
import org.exist.security.xacml.AccessContext;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.TransactionManager;
import org.exist.storage.txn.Txn;
import org.exist.test.TestConstants;
import org.exist.util.Configuration;
import org.exist.util.ConfigurationHelper;
import org.exist.util.MimeTable;
import org.exist.util.MimeType;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.XQuery;
import org.exist.xquery.value.Sequence;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.InputSource;

public class LuceneSearchTest {

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
        metas1.put("status", "draft");
    }

    private static Map<String, String> metas2 = new HashMap<String, String>();
    static {
        metas2.put("status", "final");
    }

    private static BrokerPool pool;
    private static Collection root;
    private Boolean savedConfig;

    @Test
    public void inlineAndIgnore() {
        System.out.println("Test simple queries ...");
        configureAndStore(COLLECTION_CONFIG5, 
                new Resource[] {
                    new Resource("test1.xml", XML5, metas1),
                    new Resource("test2.xml", XML5, metas2),
                });
        DBBroker broker = null;
        try {
            broker = pool.get(pool.getSecurityManager().getSystemSubject());
            assertNotNull(broker);

            XQuery xquery = broker.getXQueryService();
            assertNotNull(xquery);
            Sequence seq = xquery.execute("/article[ft:query(head, 'title')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(2, seq.getItemCount());

            assertNotNull(xquery);
            seq = xquery.execute("/article[ft:query(head, 'title AND status:draft')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(1, seq.getItemCount());

            assertNotNull(xquery);
            seq = xquery.execute("/article[ft:query(head, 'title AND status:final')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(1, seq.getItemCount());

            assertNotNull(xquery);
            seq = xquery.execute("/article[ft:query(head, 'title AND status:(final OR draft)')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(2, seq.getItemCount());

            assertNotNull(xquery);
            seq = xquery.execute("/article[ft:query(head, 'title AND status:another')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(0, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(p, 'highlighted')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(2, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(p, 'mixed')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(2, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(p, 'mix')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(0, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(p, 'dangerous')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(2, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(p, 'ous')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(0, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(p, 'danger')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(0, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(p, 'note')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(0, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(., 'highlighted')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(2, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(., 'mixed')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(2, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(., 'dangerous')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(2, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(., 'warnings')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(2, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(., 'danger')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(0, seq.getItemCount());

            seq = xquery.execute("/article[ft:query(., 'note')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(0, seq.getItemCount());
            
            seq = xquery.execute("/article[ft:query(., 'ignore')]", null, AccessContext.TEST);
            assertNotNull(seq);
            assertEquals(0, seq.getItemCount());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            pool.release(broker);
        }
    }

    private DocumentSet configureAndStore(String configuration, String data, String docName, Map<String, String> metas) {
        DBBroker broker = null;
        TransactionManager transact = null;
        Txn transaction = null;
        MutableDocumentSet docs = new DefaultDocumentSet();
        try {
            broker = pool.get(pool.getSecurityManager().getSystemSubject());
            assertNotNull(broker);
            transact = pool.getTransactionManager();
            assertNotNull(transact);
            transaction = transact.beginTransaction();
            assertNotNull(transaction);

            MetaData md = MetaData.get();
            assertNotNull(md);

            if (configuration != null) {
                CollectionConfigurationManager mgr = pool.getConfigurationManager();
                mgr.addConfiguration(transaction, broker, root, configuration);
            }

            IndexInfo info = root.validateXMLResource(transaction, broker, XmldbURI.create(docName), data);
            assertNotNull(info);

            if (docs != null) {
                Metas docMD = md.getMetas(info.getDocument());
                if (docMD == null) {
                    docMD = md.addMetas(info.getDocument());
                }
                assertNotNull(docMD);
                
                for (Entry<String, String> entry : metas.entrySet()) {
                    docMD.put(entry.getKey(), entry.getValue());
                }
            }
            
            root.store(transaction, broker, info, data, false);

            docs.add(info.getDocument());
            
            transact.commit(transaction);
        } catch (Exception e) {
            if (transact != null)
                transact.abort(transaction);
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            pool.release(broker);
        }
        
        return docs;
    }
    
    class Resource {
        final String docName;
        final String data;
        final Map<String, String> metas;
        
        Resource(String docName, String data, Map<String, String> metas) {
            this.docName = docName;
            this.data = data;
            this.metas = metas;
        }
    }
    
    private DocumentSet configureAndStore(String configuration, Resource[] resources) {
        DBBroker broker = null;
        TransactionManager transact = null;
        Txn transaction = null;
        MutableDocumentSet docs = new DefaultDocumentSet();
        try {
            broker = pool.get(pool.getSecurityManager().getSystemSubject());
            assertNotNull(broker);
            transact = pool.getTransactionManager();
            assertNotNull(transact);
            transaction = transact.beginTransaction();
            assertNotNull(transaction);

            MetaData md = MetaData.get();
            assertNotNull(md);

            if (configuration != null) {
                CollectionConfigurationManager mgr = pool.getConfigurationManager();
                mgr.addConfiguration(transaction, broker, root, configuration);
            }
            
            for (Resource resource : resources) {
                IndexInfo info = root.validateXMLResource(transaction, broker, XmldbURI.create(resource.docName), resource.data);
                assertNotNull(info);
    
                if (docs != null) {
                    Metas docMD = md.getMetas(info.getDocument());
                    if (docMD == null) {
                        docMD = md.addMetas(info.getDocument());
                    }
                    assertNotNull(docMD);
                    
                    for (Entry<String, String> entry : resource.metas.entrySet()) {
                        docMD.put(entry.getKey(), entry.getValue());
                    }
                }
                
                root.store(transaction, broker, info, resource.data, false);
    
                docs.add(info.getDocument());
            }
            
            transact.commit(transaction);
        } catch (Exception e) {
            if (transact != null)
                transact.abort(transaction);
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            pool.release(broker);
        }
        
        return docs;
    }


    private DocumentSet configureAndStore(String configuration, String directory) {
        DBBroker broker = null;
        TransactionManager transact = null;
        Txn transaction = null;
        MutableDocumentSet docs = new DefaultDocumentSet();
        try {
            broker = pool.get(pool.getSecurityManager().getSystemSubject());
            assertNotNull(broker);
            transact = pool.getTransactionManager();
            assertNotNull(transact);
            transaction = transact.beginTransaction();
            assertNotNull(transaction);

            if (configuration != null) {
                CollectionConfigurationManager mgr = pool.getConfigurationManager();
                mgr.addConfiguration(transaction, broker, root, configuration);
            }

            File file = new File(directory);
            File[] files = file.listFiles();
            MimeTable mimeTab = MimeTable.getInstance();
            for (int j = 0; j < files.length; j++) {
                MimeType mime = mimeTab.getContentTypeFor(files[j].getName());
                if(mime != null && mime.isXMLType()) {
                    System.out.println("Storing document " + files[j].getName());
                    InputSource is = new InputSource(files[j].getAbsolutePath());
                    IndexInfo info =
                            root.validateXMLResource(transaction, broker, XmldbURI.create(files[j].getName()), is);
                    assertNotNull(info);
                    is = new InputSource(files[j].getAbsolutePath());
                    root.store(transaction, broker, info, is, false);
                    docs.add(info.getDocument());
                }
            }
            transact.commit(transaction);
        } catch (Exception e) {
            if (transact != null)
                transact.abort(transaction);
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            pool.release(broker);
        }
        return docs;
    }

    @Before
    public void setup() {
        DBBroker broker = null;
        TransactionManager transact = null;
        Txn transaction = null;
        try {
            broker = pool.get(pool.getSecurityManager().getSystemSubject());
            assertNotNull(broker);
            transact = pool.getTransactionManager();
            assertNotNull(transact);
            transaction = transact.beginTransaction();
            assertNotNull(transaction);

            root = broker.getOrCreateCollection(transaction, TestConstants.TEST_COLLECTION_URI);
            assertNotNull(root);
            broker.saveCollection(transaction, root);

            transact.commit(transaction);

            Configuration config = BrokerPool.getInstance().getConfiguration();
            savedConfig = (Boolean) config.getProperty(Indexer.PROPERTY_PRESERVE_WS_MIXED_CONTENT);
            config.setProperty(Indexer.PROPERTY_PRESERVE_WS_MIXED_CONTENT, Boolean.TRUE);
        } catch (Exception e) {
            if (transact != null)
                transact.abort(transaction);
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (pool != null)
                pool.release(broker);
        }
    }

    @After
    public void cleanup() {
        BrokerPool pool = null;
        DBBroker broker = null;
        TransactionManager transact = null;
        Txn transaction = null;
        try {
            pool = BrokerPool.getInstance();
            assertNotNull(pool);
            broker = pool.get(pool.getSecurityManager().getSystemSubject());
            assertNotNull(broker);
            transact = pool.getTransactionManager();
            assertNotNull(transact);
            transaction = transact.beginTransaction();
            assertNotNull(transaction);

            Collection collConfig = broker.getOrCreateCollection(transaction,
                XmldbURI.create(XmldbURI.CONFIG_COLLECTION + "/db"));
            assertNotNull(collConfig);
            broker.removeCollection(transaction, collConfig);

            if (root != null) {
                assertNotNull(root);
                broker.removeCollection(transaction, root);
            }
            transact.commit(transaction);

            Configuration config = BrokerPool.getInstance().getConfiguration();
            config.setProperty(Indexer.PROPERTY_PRESERVE_WS_MIXED_CONTENT, savedConfig);
        } catch (Exception e) {
            if (transact != null)
                transact.abort(transaction);
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (pool != null) pool.release(broker);
        }
    }

    @BeforeClass
    public static void startDB() {
        try {
            File confFile = ConfigurationHelper.lookup("conf.xml");
            Configuration config = new Configuration(confFile.getAbsolutePath());
            config.setProperty(Indexer.PROPERTY_SUPPRESS_WHITESPACE, "none");
            config.setProperty(Indexer.PRESERVE_WS_MIXED_CONTENT_ATTRIBUTE, Boolean.TRUE);
            BrokerPool.configure(1, 5, config);
            pool = BrokerPool.getInstance();
            assertNotNull(pool);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @AfterClass
    public static void stopDB() {
        TestUtils.cleanupDB();
        BrokerPool.stopAll(false);
        pool = null;
        root = null;
    }
}

