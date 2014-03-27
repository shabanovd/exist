/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-04 The eXist Project
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
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *  
 *  $Id$
 */
package org.exist.storage;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Iterator;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import org.exist.collections.Collection;
import org.exist.dom.DocumentImpl;
import org.exist.storage.lock.Lock;
import org.exist.storage.serializers.Serializer;
import org.exist.storage.txn.TransactionManager;
import org.exist.storage.txn.Txn;
import org.exist.util.Configuration;
import org.exist.util.XMLFilenameFilter;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class NIOConcurrentStoreTest extends TestCase {
	
    public static void main(String[] args) {
        TestRunner.run(NIOConcurrentStoreTest.class);
    }
    
    //TODO : revisit !
    private static String directory = "/home/dmitriy/git/exist-shabanovd/test/shakespeare";
    private static XmldbURI TEST_COLLECTION_URI = XmldbURI.ROOT_COLLECTION_URI.append("test");
    
    private static File dir = new File(directory);
    
    private BrokerPool db;
    private Collection test1, test2;
    
    public void testStore() {
    	try {
            // BrokerPool.FORCE_CORRUPTION = true;
            db = startDB();

            setupCollections();

            Thread t1 = new StoreThread1();
            t1.start();

            wait(8000);

            Thread t2 = new StoreThread2();
            t2.start();

            t1.join();
            t2.join();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
    
    public void testRead() {
        BrokerPool.FORCE_CORRUPTION = false;
        db = startDB();
        
        DBBroker broker = null;
        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());
            
            test1 = broker.getCollection(TEST_COLLECTION_URI.append("test1"));
            assertNotNull(test1);
            
            test2 = broker.getCollection(TEST_COLLECTION_URI.append("test2"));
            assertNotNull(test2);
            
            System.out.println("Contents of collection " + test1.getURI() + ":");
            
            for (Iterator<DocumentImpl> i = test1.iterator(broker); i.hasNext(); ) {
                DocumentImpl next = i.next();
                System.out.println("- " + next.getURI());
            }
	    
            System.out.println("Contents of collection " + test2.getURI() + ":");
            
            for (Iterator<DocumentImpl> i = test2.iterator(broker); i.hasNext(); ) {
                DocumentImpl next = i.next();
                System.out.println("- " + next.getURI());
            }

        } catch (Exception e) {            
            fail(e.getMessage());              
        } finally {
            db.release(broker);
        }
    }
    
    public void testSingleStore() {
        BrokerPool.FORCE_CORRUPTION = false;
        db = startDB();
        
        setupCollections();
        
        Txn txn = db.getTransactionManager().beginTransaction();

        DBBroker broker = null;
        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());
            
//            Iterator<DocumentImpl> i = test1.iterator(broker);
//            DocumentImpl doc = i.next();
//            
//            System.out.println("\nREMOVING DOCUMENT\n");
//            test1.removeXMLResource(txn, broker, doc.getFileURI());
            
            File f = new File(dir, "hamlet.xml");
            try {
                test1.storeXML(XmldbURI.create("test.xml"), new InputSource(f.toURI().toASCIIString()));

            } catch (SAXException e) {
                System.err.println("Error found while parsing document: " + f.getName() + ": " + e.getMessage());
            }
            
            txn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            txn.close();
            
            db.release(broker);
            broker = null;
        }
        
        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());
            
            Serializer serializer = broker.getSerializer();
            serializer.reset();
            
            DocumentImpl doc = broker.getXMLResource(test1.getURI().append("test.xml"), Lock.READ_LOCK);
            assertNotNull("Document '" + test1.getURI().append("test.xml") + "' should not be null", doc);
            
            String data = serializer.serialize(doc);
            assertNotNull(data);
            
            doc.getUpdateLock().release(Lock.READ_LOCK);
            
            System.out.println(data);

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            db.release(broker);
            broker = null;
        }

    }

    
    protected void setupCollections() {
        DBBroker broker = null;
        
        Txn txn = db.getTransactionManager().beginTransaction();

        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());
            
            System.out.println("Transaction started ...");
            
            Collection root = broker.getOrCreateCollection(txn, TEST_COLLECTION_URI);
            broker.saveCollection(txn, root);
            
            test1 = broker.getOrCreateCollection(txn, TEST_COLLECTION_URI.append("test1"));
            broker.saveCollection(txn, test1);
            
            test2 = broker.getOrCreateCollection(txn, TEST_COLLECTION_URI.append("test2"));
            broker.saveCollection(txn, test2);
            
            txn.commit();
            
        } catch (Exception e) {
            txn.abort();            
            fail(e.getMessage());
        } finally {
            db.release(broker);
        }
    }
    
    protected BrokerPool startDB() {
        try {
            Configuration config = new Configuration();
            BrokerPool.configure(1, 5, config);
            return BrokerPool.getInstance();
        } catch (Exception e) {            
            fail(e.getMessage());
        }
        return null;
    }

    protected void tearDown() {
        BrokerPool.stopAll(false);
    }
    
    class StoreThread1 extends Thread {
        
        public void run() {
            DBBroker broker = null;
            try {
                broker = db.get(db.getSecurityManager().getSystemSubject());
                
                System.out.println("Transaction started ...");
                XMLFilenameFilter filter = new XMLFilenameFilter();
                File files[] = dir.listFiles(filter);
                
                File f;
                // store some documents into the test collection
                for (int i = 0; i < files.length; i++) {
                    f = files[i];
                    try {
                        test1.storeXML(XmldbURI.create(f.getName()), new InputSource(f.toURI().toASCIIString()));

                    } catch (SAXException e) {
                        System.err.println("Error found while parsing document: " + f.getName() + ": " + e.getMessage());
                    }
//                    if (i % 5 == 0) {
//                        transact.commit(transaction);
//                        transaction = transact.beginTransaction();
//                    }
                }
                
                //tm.commit(txn);
                
//              Don't commit...
                //tm.getJournal().flushToLog(true);
                System.out.println("Transaction interrupted ...");
    	    } catch (Exception e) {            
    	        fail(e.getMessage()); 
            } finally {
                db.release(broker);
            }
        }
    }
    
    class StoreThread2 extends Thread {
        public void run() {
            DBBroker broker = null;
            try {
                broker = db.get(db.getSecurityManager().getSystemSubject());
                
                TransactionManager tm = db.getTransactionManager();
                Txn txn = tm.beginTransaction();
                
                System.out.println("Transaction started ...");
                
                Iterator<DocumentImpl> i = test1.iterator(broker);
                DocumentImpl doc = i.next();
                
                System.out.println("\nREMOVING DOCUMENT\n");
                test1.removeXMLResource(txn, broker, doc.getFileURI());
                
                File f = new File(dir, "hamlet.xml");
                try {
                    test1.storeXML(XmldbURI.create("test.xml"), new InputSource(f.toURI().toASCIIString()));

                } catch (SAXException e) {
                    System.err.println("Error found while parsing document: " + f.getName() + ": " + e.getMessage());
                }
                
                tm.commit(txn);
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            } finally {
                db.release(broker);
            }
        }
    }
}