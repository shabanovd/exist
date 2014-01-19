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
package org.exist.collections.triggers;

import static org.junit.Assert.*;

import org.exist.EXistException;
import org.exist.TestUtils;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.xmldb.DatabaseInstanceManager;
import org.exist.xmldb.XmldbURI;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.Resource;
import org.xmldb.api.base.XMLDBException;
import org.xmldb.api.modules.CollectionManagementService;

public class FilteringTriggerTest {

    private final static String DOCUMENT_CONTENT = 
        "<test>"
        + "<item id='1'><price>5.6</price><stock>22</stock></item>"
        + "<item id='2'><price>7.4</price><stock>43</stock></item>"
        + "<item id='3'><price>18.4</price><stock>5</stock></item>"
        + "<item id='4'><price>65.54</price><stock>16</stock></item>"
        + "</test>";

    private final static String BASE_URI = "xmldb:exist://";

    private final static String testCollection = "/db/triggers";

    @Test
    public void test() throws EXistException {

        BrokerPool db = BrokerPool.getInstance();

        AnotherTrigger trigger = new AnotherTrigger();

        db.getDocumentTriggers().add(trigger);

        DBBroker broker = null;

        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());

            Collection root = DatabaseManager.getCollection(BASE_URI + testCollection, "admin", "");

            Resource resource = root.createResource("data.xml", "XMLResource");
            resource.setContent(DOCUMENT_CONTENT);
            root.storeResource(resource);

            assertEquals(3, trigger.createDocumentEvents);

            assertEquals(26, trigger.count);

        } catch (XMLDBException e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (broker != null)
                broker.release();
        }
    }

    @After
    public void cleanDB() {
        try {
            Collection config = DatabaseManager.getCollection(BASE_URI + "/db/system/config" + testCollection, "admin", null);
            if (config != null) {
                CollectionManagementService mgmt = (CollectionManagementService) config.getService("CollectionManagementService", "1.0");
                mgmt.removeCollection(".");
            }
            Collection root = DatabaseManager.getCollection(BASE_URI + testCollection, "admin", "");
            Resource resource = root.getResource("messages.xml");
            if (resource != null) {
                root.removeResource(resource);
            }
            resource = root.getResource("data.xml");
            if (resource != null) {
                root.removeResource(resource);
            }
        } catch (XMLDBException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @BeforeClass
    public static void initDB() {
        // initialize XML:DB driver
        try {
            Class<?> cl = Class.forName("org.exist.xmldb.DatabaseImpl");
            Database database = (Database) cl.newInstance();
            database.setProperty("create-database", "true");
            DatabaseManager.registerDatabase(database);

            Collection root = DatabaseManager.getCollection(XmldbURI.LOCAL_DB, "admin", "");
            CollectionManagementService mgmt = (CollectionManagementService) root.getService("CollectionManagementService", "1.0");
            Collection testCol = mgmt.createCollection("triggers");

            for (int i = 1; i <= 2; i++) {
                mgmt = (CollectionManagementService) testCol.getService("CollectionManagementService", "1.0");
                testCol = mgmt.createCollection("sub" + i);
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @AfterClass
    public static void closeDB() {
        TestUtils.cleanupDB();
        try {
            Collection root = DatabaseManager.getCollection(XmldbURI.LOCAL_DB, "admin", "");
            DatabaseInstanceManager mgr = (DatabaseInstanceManager) root.getService("DatabaseInstanceManager", "1.0");
            mgr.shutdown();
        } catch (XMLDBException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
