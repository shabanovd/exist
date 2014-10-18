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
import org.exist.storage.MetaStorage;
import org.exist.storage.md.MetaData;
import org.exist.storage.md.Metas;
import org.exist.storage.txn.Txn;
import org.exist.test.TestConstants;
import org.exist.util.*;
import org.exist.xmldb.XmldbURI;
import org.junit.*;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

public class RCSTest {

    private static String XML1 =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
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
                    new Resource("test1.xml", XML1, null),
                    new Resource("test2.xml", XML2, null),
                    new Resource("test3.xml", XML3, null),
                    new Resource("test4.xml", XML4, null),
                    new Resource("test5.xml", XML5, null),
                    new Resource("test6.xml", XML6, null),
                    new Resource("test7.xml", XML7, null),
                    new Resource("test8.xml", XML8, null),
            });

        try (DBBroker broker = pool.authenticate("admin", "")) {
            assertNotNull(broker);

            Handler h = new TestHandler();

            RCSManager manager = RCSManager.get();

            manager.snapshot(root, h);

            manager.restore(broker.getDataFolder().toPath().resolve("RCS"), h);

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
                        new Resource("test1.xml", XML1, null),
                        new Resource("test2.xml", XML2, null),
                        new Resource("test3.xml", XML3, null),
                        new Resource("test4.xml", XML4, null),
                        new Resource("test5.xml", XML5, null),
                        new Resource("test6.xml", XML6, null),
                        new Resource("test7.xml", XML7, null),
                        new Resource("test8.xml", XML8, null),
                });

        try (DBBroker broker = pool.authenticate("admin", "")) {
            assertNotNull(broker);

            Handler h = new TestHandler();

            RCSManager rcs = RCSManager.get();

            XmldbURI colURL = root.getURI();

            try (CommitWriter commit = rcs.commit(h)) {

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

            RCSResource resource = rcs.resource(doc1uuid);
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

                        IndexInfo info = root.validateXMLResource(txn, broker, XmldbURI.create(resource.docName), resource.data);
                        assertNotNull(info);

                        root.store(txn, broker, info, resource.data, false);

                        docs.add(info.getDocument());

                        //broker.reindexXMLResource(transaction, info.getDocument());
                    } else {

                        final MimeTable mimeTable = MimeTable.getInstance();

                        final MimeType mimeType = mimeTable.getContentTypeFor(resource.docName);

                        InputStream is = new StringInputStream(resource.data);

                        XmldbURI name = XmldbURI.create(resource.docName);

                        BinaryDocument binary = root.validateBinaryResource(txn, broker, name, is, mimeType.toString(), (long) -1, (Date) null, (Date) null);

                        binary = root.addBinaryResource(txn, broker, name, is, mimeType.getName(), -1, (Date) null, (Date) null);

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
        final String docName;
        final String data;
        final Map<String, String> metas;
        final String type;

        Resource(String docName, String data, Map<String, String> metas) {
            this.docName = docName;
            this.data = data;
            this.metas = metas;

            type = "XML";
        }

        Resource(String type, String docName, String data, Map<String, String> metas) {
            this.type = type;
            this.docName = docName;
            this.data = data;
            this.metas = metas;
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

