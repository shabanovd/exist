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
 */
package org.exist;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.exist.xmldb.DatabaseInstanceManager;
import org.exist.xmldb.XmldbURI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.modules.CollectionManagementService;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class CommonAbstract {
	
	protected final static String TEST_COLLECTION = "testXQueryTrigger";
	
	protected static Collection root;
	protected static Collection testCollection;

    @BeforeClass
    public static void startDB() {
        try {
            // initialize driver
            Class<?> cl = Class.forName("org.exist.xmldb.DatabaseImpl");
            Database database = (Database) cl.newInstance();
            database.setProperty("create-database", "true");
            DatabaseManager.registerDatabase(database);

            root = DatabaseManager.getCollection(XmldbURI.LOCAL_DB, "admin", "");
            CollectionManagementService service = (CollectionManagementService) root.getService("CollectionManagementService", "1.0");
            testCollection = service.createCollection(TEST_COLLECTION);
            assertNotNull(testCollection);
        } catch (Throwable e) {
            e.printStackTrace();
        	fail(e.getMessage());
        }
    }

    @AfterClass
    public static void shutdownDB() {
        TestUtils.cleanupDB();
        try {
            Collection root = DatabaseManager.getCollection("xmldb:exist://" + XmldbURI.ROOT_COLLECTION, "admin", "");
            DatabaseInstanceManager mgr = (DatabaseInstanceManager) root.getService("DatabaseInstanceManager", "1.0");
            mgr.shutdown();
        } catch (Throwable e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        root = null;
        testCollection = null;
    }
}
