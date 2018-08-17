/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2018 The eXist Project
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
package org.exist.protocolhandler;

import static org.exist.xmldb.XmldbLocalTests.ADMIN_PWD;
import static org.exist.xmldb.XmldbLocalTests.ADMIN_UID;
import static org.exist.xmldb.XmldbLocalTests.GUEST_UID;
import static org.exist.xmldb.XmldbLocalTests.ROOT_URI;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.commons.io.IOUtils;
import org.exist.security.Account;
import org.exist.security.Permission;
import org.exist.xmldb.DatabaseInstanceManager;
import org.exist.xmldb.UserManagementService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Node;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.Resource;
import org.xmldb.api.base.ResourceSet;
import org.xmldb.api.base.XMLDBException;
import org.xmldb.api.modules.BinaryResource;
import org.xmldb.api.modules.CollectionManagementService;
import org.xmldb.api.modules.XMLResource;
import org.xmldb.api.modules.XQueryService;

public class ProtocolHandlerTest {

    private final static String TEST_COLLECTION = "testProtocolHandler";

    private final static String baseURI = "/db/"+TEST_COLLECTION+"/";

    private final static String xmlDocName = "text.xml";
    private final static String xmlData = "<text/>";

    private final static String binDocName = "text.bin";
    private final static String binData = "text";

    @Test
    public void xmlStreams() throws Exception {
        upload(xmlDocName, xmlData);

        XMLResource doc = (XMLResource) content(xmlDocName);

        assertEquals(xmlData, doc.getContent());

        List<String> lines = download(xmlDocName);

        assertEquals(1, lines.size());
        assertEquals(xmlData, lines.get(0));
    }

    @Test
    public void binaryStreams() throws Exception {
        upload(binDocName, binData);

        BinaryResource doc = (BinaryResource) content(binDocName);

        assertEquals(binData, new String((byte[])doc.getContent()));

        List<String> lines = download(binDocName);

        assertEquals(1, lines.size());
        assertEquals(binData, lines.get(0));
    }

    private URL url(String name) throws MalformedURLException {
        return new URL("xmldb://"+baseURI+name);
    }

    private void upload(String name, String data) throws IOException {
        try (OutputStream out = url(name).openConnection().getOutputStream()) {
            out.write(data.getBytes());
        }
    }

    private List<String> download(String name) throws IOException {
        try (InputStream in = url(name).openConnection().getInputStream()) {
            return IOUtils.readLines(in);
        }
    }

    private Resource content(String name) throws XMLDBException {
        Collection col = DatabaseManager.getCollection(ROOT_URI+"/"+TEST_COLLECTION, ADMIN_UID, ADMIN_PWD);

        return col.getResource(name);
    }

    @Before
    public void setUp() throws Exception {
        // initialize driver
        Class<?> cl = Class.forName("org.exist.xmldb.DatabaseImpl");
        Database database = (Database) cl.newInstance();
        database.setProperty("create-database", "true");
        DatabaseManager.registerDatabase(database);

        Collection root = DatabaseManager.getCollection(ROOT_URI, ADMIN_UID, ADMIN_PWD);
        CollectionManagementService service = (CollectionManagementService) root.getService("CollectionManagementService", "1.0");
        Collection testCollection = service.createCollection(TEST_COLLECTION);
        UserManagementService ums = (UserManagementService) testCollection.getService("UserManagementService", "1.0");
        // change ownership to guest
        Account guest = ums.getAccount(GUEST_UID);
        ums.chown(guest, guest.getPrimaryGroup());
        ums.chmod(Permission.DEFAULT_COLLECTION_PERM);
    }

    @After
    public void tearDown() throws XMLDBException {

        //delete the test collection
        Collection root = DatabaseManager.getCollection(ROOT_URI, ADMIN_UID, ADMIN_PWD);
        CollectionManagementService service = (CollectionManagementService)root.getService("CollectionManagementService", "1.0");
        service.removeCollection(TEST_COLLECTION);

        //shutdownDB the db
        DatabaseInstanceManager dim = (DatabaseInstanceManager) root.getService("DatabaseInstanceManager", "1.0");
        dim.shutdown();
    }
}
