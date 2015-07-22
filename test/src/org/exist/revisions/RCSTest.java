/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2014 The eXist Project
 * http://exist-db.org
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.revisions;

import org.apache.commons.io.IOUtils;
import org.apache.tools.ant.filters.StringInputStream;
import org.exist.Indexer;
import org.exist.TestUtils;
import org.exist.collections.Collection;
import org.exist.collections.CollectionConfigurationManager;
import org.exist.collections.IndexInfo;
import org.exist.dom.*;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.md.Meta;
import org.exist.storage.md.MetaData;
import org.exist.storage.md.Metas;
import org.exist.storage.serializers.EXistOutputKeys;
import org.exist.storage.serializers.Serializer;
import org.exist.storage.txn.Txn;
import org.exist.test.TestConstants;
import org.exist.util.*;
import org.exist.xmldb.XmldbURI;
import org.junit.*;
import org.w3c.dom.DocumentType;
import org.xml.sax.SAXException;

import javax.xml.transform.OutputKeys;
import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.*;

import static org.junit.Assert.*;

public class RCSTest {

    private static String XML1 =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\" \"http://www.w3.org/TR/html4/strict.dtd\">\n" +
            "<section>\n" +
            "    <head>The title in big letters</head>\n" +
            "    <p rend=\"center\">A simple paragraph with <hi>just</hi> text in it.</p>\n" +
            "    <p rend=\"right\">paragraphs with <span>mix</span>\n" +
            "        <span>ed</span> content are <span>danger</span>ous.</p>\n" +
            "</section>";

    private static String XML1_1 =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<!DOCTYPE topic PUBLIC \"-//OASIS//DTD DITA Topic//EN\"\n" +
            "\"topic.dtd\">" +
            "<section>\n" +
            "    <head>The title in big letters</head>\n" +
            "    <p rend=\"center\">A simple paragraph with <hi>just</hi> text in it.</p>\n" +
            "    <p rend=\"center\">A simple paragraph with <hi>just</hi> text in it.</p>\n" +
            "    <p rend=\"right\">paragraphs with <span>mix</span>\n" +
            "        <span>ed</span> content are <span>danger</span>ous.</p>\n" +
            "</section>";

    private static String XML1_2 =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<!DOCTYPE topic PUBLIC \"-//OASIS//DTD DITA Topic//EN\" \"topic.dtd\">\n" +
            "<section>\n" +
            "    <head>The title in big letters</head>\n" +
            "    <p rend=\"center\">A simple paragraph with <hi>just</hi> text in it.</p>\n" +
            "    <p rend=\"right\">paragraphs with <span>mix</span>\n" +
            "        <span>ed</span> content are <span>danger</span>ous.</p>\n" +
            "</section>";

    private static String XML2 =
            "<test>" +
            "   <item id='1' attr='attribute'><description>Chair</description></item>" +
            "   <item id='2'><description>Table</description>\n<condition>good</condition></item>" +
            "   <item id='3'><description>Cabinet</description>\n<condition>bad</condition></item>" +
            "</test>";

    private static String XML3 =
            "<section>" +
            "   <head>TITLE IN UPPERCASE LETTERS</head>" +
            "   <p>UPPERCASE PARAGRAPH</p>" +
            "</section>";

    private static String XML4 =
            "<test><a>A X</a><b><c>B X</c> C</b></test>";

    private static String XML5 =
            "<article>" +
            "   <head>The <b>title</b>of it</head>" +
            "   <p>A simple paragraph with <hi>highlighted</hi> text <note>and a note</note> " +
            "       in it.</p>" +
            "   <p>Paragraphs with <s>mix</s><s>ed</s> content are <s>danger</s>ous.</p>" +
            "   <p><note1>ignore</note1> <s2>warn</s2>ings</p>" +
            "</article>";

    private static String XML6 =
            "<a>" +
            "   <b>AAA</b>" +
            "   <c>AAA</c>" +
            "   <b>AAA</b>" +
            "</a>";

    private static String XML7 =
        "<section>" +
        "   <head>Query Test</head>" +
        "   <p>Eine wunderbare Heiterkeit hat meine ganze Seele eingenommen, gleich den " +
        "   süßen Frühlingsmorgen, die ich mit ganzem Herzen genieße. Ich bin allein und " +
        "   freue mich meines Lebens in dieser Gegend, die für solche Seelen geschaffen " +
        "   ist wie die meine. Ich bin so glücklich, mein Bester, so ganz in dem Gefühle " +
        "   von ruhigem Dasein versunken, daß meine Kunst darunter leidet.</p>" +
        "</section>";

    private static String XML8 =
            "<a>" +
            "   <b class=' class title '>AAA</b>" +
            "   <c class=' element title '>AAA</c>" +
            "   <b class=' element '>AAA</b>" +
            "</a>";

    private static BrokerPool pool;
    private static Collection root;

    @Test
    public void testSnapshot() {
        System.out.println("Test snapshot ...");

        configureAndStore(null,
            new Resource[] {
                    new Resource("test1.xml", XML1),
                    new Resource("test2.xml", XML2),
                    new Resource("test3.xml", XML3),
                    new Resource("test4.xml", XML4),
                    new Resource("test5.xml", XML5),
                    new Resource("test6.xml", XML6),
                    new Resource("test7.xml", XML7),
                    new Resource("test8.xml", XML8),
            });

        try (DBBroker broker = pool.authenticate("admin", "")) {
            assertNotNull(broker);

            Handler h = new TestHandler();

            RCSManager manager = RCSManager.get();

            RCSHolder holder = manager.getOrCreateHolder("test");

            holder.snapshot(root, h);

            holder.restore(holder.rcFolder, h);

            System.out.println("Test PASSED.");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCommits() {
        System.out.println("Test commits ...");

        configureAndStore(null,
                new Resource[] {
                        new Resource("test1.xml", XML1),
                        new Resource("test2.xml", XML2),
                        new Resource("test3.xml", XML3),
                        new Resource("test4.xml", XML4),
                        new Resource("test5.xml", XML5),
                        new Resource("test6.xml", XML6),
                        new Resource("test7.xml", XML7),
                        new Resource("test8.xml", XML8),
                });

        try (DBBroker broker = pool.authenticate("admin", "")) {
            assertNotNull(broker);

            Handler h = new TestHandler();

            RCSManager manager = RCSManager.get();

            RCSHolder holder = manager.getOrCreateHolder("test");

            XmldbURI colURL = root.getURI();

            try (CommitWriter commit = holder.commit(h)) {

                commit
                    .author("somebody")
                    .message("here go message <possible>xml</possible>")
                    .create(colURL.append("test1.xml"))
                    .create(colURL.append("test2.xml"))
                    .create(colURL.append("test3.xml"))
                    .create(colURL.append("test4.xml"))
                    .create(colURL.append("test5.xml"))
                    .create(colURL.append("test6.xml"))
                    .create(colURL.append("test7.xml"))
                    .create(colURL.append("test8.xml"))

                    .done();
            }

            MetaData md = MetaData.get();

            Metas metas = md.getMetas(colURL.append("test1.xml"));
            assertNotNull(metas);

            String doc1uuid = metas.getUUID();
            assertNotNull(doc1uuid);

            RCSResource resource = holder.resource(doc1uuid);
            assertNotNull(resource);

            Revision rev = resource.lastRevision();
            assertNotNull(rev);

            assertTrue(rev.isXML());
            assertFalse(rev.isBinary());
            assertFalse(rev.isCollection());
            assertFalse(rev.isDeleted());

            StringWriter writer = new StringWriter();

            try (InputStream is = rev.getData()) {
                IOUtils.copy(is, writer);
            }
            String data = writer.toString();

            assertEquals(XML1, data);

            System.out.println("Test PASSED.");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRestoreMimeType() {
        System.out.println("Test ...");

        String MT1 = "text/html";
        String MT2 = "application/xml";

        XmldbURI R1 = XmldbURI.create("test1.xml");

        String N1 = "test";
        String V1 = "t1";
        String V2 = "t2";

        Map<String, String> map1 = new HashMap<>();
        map1.put(N1, V1);

        Map<String, String> map2 = new HashMap<>();
        map2.put(N1, V2);

        configureAndStore(null,
            new Resource[]{
                (new Resource(R1, XML1, map1)).mimeType(MT1),
            });

        try (DBBroker broker = pool.authenticate("admin", "")) {
            assertNotNull(broker);

            DocumentImpl d1 = root.getDocument(broker, R1);
            assertEquals(MT1, d1.getMetadata().getMimeType());

            Handler h = new TestHandler();

            RCSManager manager = RCSManager.get();

            RCSHolder holder = manager.getOrCreateHolder("test");

            XmldbURI colURL = root.getURI();

            try (CommitWriter commit = holder.commit(h)) {
                commit
                    .create(colURL.append(R1))
                    .done();
            }

            MetaData md = MetaData.get();

            Metas metas = md.getMetas(colURL.append(R1));
            assertNotNull(metas);

            String doc1uuid = metas.getUUID();
            assertNotNull(doc1uuid);

            Meta mt = metas.get(N1);
            assertNotNull(mt);
            assertEquals(V1, mt.getValue());

            RCSResource resource = holder.resource(doc1uuid);
            assertNotNull(resource);

            Revision rev = resource.lastRevision();
            assertNotNull(rev);

            assertTrue(rev.isXML());
            assertFalse(rev.isBinary());
            assertFalse(rev.isCollection());
            assertFalse(rev.isDeleted());

            StringWriter writer = new StringWriter();

            try (InputStream is = rev.getData()) {
                IOUtils.copy(is, writer);
            }
            assertEquals(XML1, writer.toString());

            configureAndStore(null,
                new Resource[]{
                    (new Resource(R1, XML2, map2)).mimeType(MT2),
                });

            metas = md.getMetas(colURL.append(R1));
            assertNotNull(metas);

            assertEquals(doc1uuid, metas.getUUID());

            mt = metas.get(N1);
            assertNotNull(mt);
            assertEquals(V2, mt.getValue());

            d1 = root.getDocument(broker, R1);
            assertEquals(MT2, d1.getMetadata().getMimeType());

            //restore revision
            rev.restore(broker, null);

            d1 = root.getDocument(broker, R1);
            assertEquals(MT1, d1.getMetadata().getMimeType());

            assertEquals(XML1, read(broker, d1));

            metas = md.getMetas(colURL.append(R1));
            assertEquals(doc1uuid, metas.getUUID());

            mt = metas.get(N1);
            assertNotNull(mt);
            assertEquals(V1, mt.getValue());

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRestoreDoctype() {
        System.out.println("Test ...");

        XmldbURI R1 = XmldbURI.create("test1.xml");

        configureAndStore(null,
            new Resource[]{
                (new Resource(R1, XML1)),
            });

        try (DBBroker broker = pool.authenticate("admin", "")) {
            assertNotNull(broker);

            DocumentImpl d1 = root.getDocument(broker, R1);
            DocumentType docType  = d1.getDoctype();

            assertEquals("HTML", docType.getName());
            assertEquals("-//W3C//DTD HTML 4.01//EN", docType.getPublicId());
            assertEquals("http://www.w3.org/TR/html4/strict.dtd", docType.getSystemId());

            Handler h = new TestHandler();

            RCSManager manager = RCSManager.get();

            RCSHolder holder = manager.getOrCreateHolder("test");

            XmldbURI colURL = root.getURI();

            try (CommitWriter commit = holder.commit(h)) {
                commit
                    .create(colURL.append(R1))
                    .done();
            }

            MetaData md = MetaData.get();

            Metas metas = md.getMetas(colURL.append(R1));
            assertNotNull(metas);

            String doc1uuid = metas.getUUID();
            assertNotNull(doc1uuid);

            RCSResource resource = holder.resource(doc1uuid);
            assertNotNull(resource);

            Revision rev = resource.lastRevision();
            assertNotNull(rev);

            StringWriter writer = new StringWriter();
            try (InputStream is = rev.getData()) {
                IOUtils.copy(is, writer);
            }
            assertEquals(XML1, writer.toString());

            configureAndStore(null,
                new Resource[]{
                    (new Resource(R1, XML1_1)),
                });

            d1 = root.getDocument(broker, R1);
            docType  = d1.getDoctype();

            assertEquals("topic", docType.getName());
            assertEquals("-//OASIS//DTD DITA Topic//EN", docType.getPublicId());
            assertEquals("topic.dtd", docType.getSystemId());

            //restore revision
            rev.restore(broker, null);

            d1 = root.getDocument(broker, R1);
            docType  = d1.getDoctype();

            assertEquals("HTML", docType.getName());
            assertEquals("-//W3C//DTD HTML 4.01//EN", docType.getPublicId());
            assertEquals("http://www.w3.org/TR/html4/strict.dtd", docType.getSystemId());

            assertEquals(XML1, read(broker, d1));

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRestoreDoNotOverwriteDoctype() {
        System.out.println("Test ...");

        XmldbURI R1 = XmldbURI.create("test1.xml");

        configureAndStore(null,
            new Resource[]{
                (new Resource(R1, XML1)),
            });

        try (DBBroker broker = pool.authenticate("admin", "")) {
            assertNotNull(broker);

            DocumentImpl d1 = root.getDocument(broker, R1);
            DocumentType docType  = d1.getDoctype();

            assertEquals("HTML", docType.getName());
            assertEquals("-//W3C//DTD HTML 4.01//EN", docType.getPublicId());
            assertEquals("http://www.w3.org/TR/html4/strict.dtd", docType.getSystemId());

            Handler h = new TestHandler();

            RCSManager manager = RCSManager.get();

            RCSHolder holder = manager.getOrCreateHolder("test");

            XmldbURI colURL = root.getURI();

            try (CommitWriter commit = holder.commit(h)) {
                commit
                    .create(colURL.append(R1))
                    .done();
            }

            MetaData md = MetaData.get();

            Metas metas = md.getMetas(colURL.append(R1));
            assertNotNull(metas);

            String doc1uuid = metas.getUUID();
            assertNotNull(doc1uuid);

            RCSResource resource = holder.resource(doc1uuid);
            assertNotNull(resource);

            Revision rev = resource.lastRevision();
            assertNotNull(rev);

            StringWriter writer = new StringWriter();
            try (InputStream is = rev.getData()) {
                IOUtils.copy(is, writer);
            }
            assertEquals(XML1, writer.toString());

            configureAndStore(null,
                new Resource[]{
                    (new Resource(R1, XML1_1)),
                });

            d1 = root.getDocument(broker, R1);
            docType  = d1.getDoctype();

            assertEquals("topic", docType.getName());
            assertEquals("-//OASIS//DTD DITA Topic//EN", docType.getPublicId());
            assertEquals("topic.dtd", docType.getSystemId());

            Map<String, String> params = new HashMap<>();
            params.put(Constants.RESTORE_DOCTYPE, Constants.NO);
            //restore revision
            rev.restore(broker, params, null);

            d1 = root.getDocument(broker, R1);
            docType  = d1.getDoctype();

            assertEquals("topic", docType.getName());
            assertEquals("-//OASIS//DTD DITA Topic//EN", docType.getPublicId());
            assertEquals("topic.dtd", docType.getSystemId());

            assertEquals(XML1_2, read(broker, d1));

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private String read(DBBroker broker, DocumentImpl document) throws SAXException {
        HashMap<String, Object> props = new HashMap<>();
        props.put(OutputKeys.OMIT_XML_DECLARATION, "no");
        props.put(EXistOutputKeys.OUTPUT_DOCTYPE, "yes");

        final Serializer serializer = broker.getSerializer();
        //serializer.setUser(user);
        serializer.setProperties(props);
        return serializer.serialize(document);
    }

    class TestHandler implements Handler {

        @Override
        public void processed(XmldbURI uri) {
            System.out.println("processed "+uri);
        }

        @Override
        public void error(XmldbURI uri, Exception e) {
            e.printStackTrace();
            System.out.println("error1 "+uri);
        }

        @Override
        public void error(XmldbURI uri, String msg) {
            System.out.println("error2 "+uri+"\n"+msg);
        }

        @Override
        public void error(Path location, Exception e) {
            e.printStackTrace();
            System.out.println("error3 "+location);
        }

        @Override
        public void error(String id, String msg) {
            System.out.println("error4 "+id+" "+msg);
        }
    }

    protected DocumentSet configureAndStore(String configuration, Resource[] resources) {

        MetaData md = MetaData.get();
        assertNotNull(md);

        MutableDocumentSet docs = new DefaultDocumentSet();

        try (DBBroker broker = pool.get(pool.getSecurityManager().getSystemSubject())) {
            assertNotNull(broker);

            try (Txn txn = broker.beginTx()) {
                assertNotNull(txn);

                if (configuration != null) {
                    CollectionConfigurationManager mgr = pool.getConfigurationManager();
                    mgr.addConfiguration(txn, broker, root, configuration);
                }

                for (Resource resource : resources) {

                    XmldbURI docURL = root.getURI().append(resource.docName);

                    if (resource.metas != null) {

                        Metas docMD = md.getMetas(docURL);
                        if (docMD == null) {
                            docMD = md.addMetas(docURL);
                        }
                        assertNotNull(docMD);

                        for (Map.Entry<String, String> entry : resource.metas.entrySet()) {
                            docMD.put(entry.getKey(), entry.getValue());
                        }
                    }

                    if ("XML".equals(resource.type)) {

                        IndexInfo info = root.validateXMLResource(txn, broker, resource.docName, resource.data);
                        assertNotNull(info);

                        if (resource.mimeType != null)
                            info.getDocument().getMetadata().setMimeType(resource.mimeType);

                        root.store(txn, broker, info, resource.data, false);

                        docs.add(info.getDocument());

                        //broker.reindexXMLResource(transaction, info.getDocument());
                    } else {

                        final MimeTable mimeTable = MimeTable.getInstance();

                        final MimeType mimeType = mimeTable.getContentTypeFor(resource.docName);

                        InputStream is = new StringInputStream(resource.data);

                        BinaryDocument binary = root.validateBinaryResource(txn, broker, resource.docName, is, mimeType.toString(), (long) -1, (Date) null, (Date) null);

                        binary = root.addBinaryResource(txn, broker, resource.docName, is, mimeType.getName(), -1, (Date) null, (Date) null);

                        docs.add(binary);
                    }

                }

                txn.success();
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        return docs;
    }

    protected class Resource {
        final XmldbURI docName;
        final String data;
        final Map<String, String> metas;
        final String type;
        String mimeType = null;

        Resource(String docName, String data) {
            this(XmldbURI.create(docName), data);
        }

        Resource(XmldbURI docName, String data) {
            this.docName = docName;
            this.data = data;
            metas = null;
            type = "XML";
        }

        Resource(XmldbURI docName, String data, Map<String, String> metas) {
            this.docName = docName;
            this.data = data;
            this.metas = metas;

            type = "XML";
        }

        Resource(XmldbURI docName, String data, Map<String, String> metas, String type) {
            this.docName = docName;
            this.data = data;
            this.metas = metas;
            this.type = type;
        }

        public Resource mimeType(String mimeType) {
            this.mimeType = mimeType;
            return this;
        }
    }

    @Before
    public void setup() {

        try (DBBroker broker = pool.authenticate("admin", "")) {
            assertNotNull(broker);

            try (Txn txn = broker.beginTx()) {
                assertNotNull(txn);

                root = broker.getOrCreateCollection(txn, TestConstants.TEST_COLLECTION_URI);
                assertNotNull(root);
                broker.saveCollection(txn, root);

                txn.success();
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @After
    public void cleanup() {
        BrokerPool pool;

        try {
            pool = BrokerPool.getInstance();
            assertNotNull(pool);

            try (DBBroker broker = pool.authenticate("admin", "")) {
                assertNotNull(broker);

                try (Txn txn = broker.beginTx()) {
                    assertNotNull(txn);

                    if (root != null) {
                        broker.removeCollection(txn, root);
                    }

                    txn.success();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
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

