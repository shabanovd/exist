/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2016 The eXist Project
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
package org.exist.xquery.restore;

import org.apache.log4j.Logger;
import org.exist.Database;
import org.exist.TrashManager;
import org.exist.backup.BackupDescriptor;
import org.exist.backup.FileSystemBackupDescriptor;
import org.exist.backup.ZipArchiveBackupDescriptor;
import org.exist.backup.restore.SystemImportHandler;
import org.exist.backup.restore.listener.RestoreListener;
import org.exist.collections.Collection;
import org.exist.collections.triggers.TriggerException;
import org.exist.config.ConfigurationException;
import org.exist.dom.DocumentImpl;
import org.exist.security.AuthenticationException;
import org.exist.security.PermissionDeniedException;
import org.exist.security.Subject;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.TransactionException;
import org.exist.storage.txn.Txn;
import org.exist.util.EXistInputSource;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xmldb.api.base.ErrorCodes;
import org.xmldb.api.base.XMLDBException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Restore 
 *
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class SystemImport {
    
    public final static Logger LOG = Logger.getLogger( SystemImport.class );

    private static AtomicInteger processing = new AtomicInteger();
    public static boolean isRunning() {
        return processing.get() == 0;
    }

    private Database db;
    
    public SystemImport(Database db) {
        this.db = db;
    }

    public void restore(RestoreListener listener, String username, Object credentials, String newCredentials, File f, String uri) throws Exception {

        ImportAnalyzer analyzer = null;

        //login
        final DBBroker broker = db.authenticate(username, credentials);
        processing.incrementAndGet();
        try {
            //set the new password
            if (newCredentials != null) {
                setAdminCredentials(broker, newCredentials);
            }

            listener.info("restore system collection");

            restoreSystemCollection(listener, broker, getSystemBackupDescriptors(f), uri, true);

            listener.info("parse backup");

            analyzer = parseBackup(listener, broker, getBackupDescriptors(f), uri, true);

            listener.info("start analysis");

            analyzer.analysis();

            listener.info("analysis done ["+analyzer.stopError+"]:\n" +
                    "create cols = "+analyzer.createCols.size()+"; docs = "+analyzer.createDocs.size()+"\n" +
                    "delete cols = "+analyzer.deleteCols.size()+"; docs = "+analyzer.deleteDocs.size()+"\n" +
                    "move   cols = "+analyzer.moveCols.size()+"; docs = "+analyzer.moveDocs.size()+"\n" +
                    "rename = "+analyzer.replace.size());

            listener.info("remove documents");
            deleteDocuments(listener, broker, analyzer.deleteDocs);

            listener.info("remove empty collections");
            deleteEmptyCollections(listener, broker, analyzer.deleteCols);

            listener.info("collection to remove = "+analyzer.deleteCols.size());

            listener.info("move documents (1)");
            moveDocuments(listener, broker, analyzer.moveDocs);

            listener.info("move collections (1)");
            moveCollections(listener, broker, analyzer);

            listener.info("move documents (2)");
            moveDocuments(listener, broker, analyzer.moveDocs);

            listener.info("move collections (2)");
            moveCollections(listener, broker, analyzer);

            listener.info("create collections & documents");
            createPhase(listener, broker, getBackupDescriptors(f), uri, true, analyzer);

            listener.info("move documents (3)");
            moveDocuments(listener, broker, analyzer.moveDocs);

            listener.info("move collections (3)");
            moveCollections(listener, broker, analyzer);

            finalPhase(listener, broker, getBackupDescriptors(f), uri, true, analyzer);


        } finally {
            processing.decrementAndGet();
            db.release(broker);

            if (analyzer != null) {
                listener.info("done [" + analyzer.stopError + "]:\n" +
                        "create cols = " + analyzer.createCols.size() + ";\tdocs = " + analyzer.createDocs.size() + "\n" +
                        "delete cols = " + analyzer.deleteCols.size() + ";\tdocs = " + analyzer.deleteDocs.size() + "\n" +
                        "move   cols = " + analyzer.moveCols.size() + ";\tdocs = " + analyzer.moveDocs.size() + "\n" +
                        "replace = " + analyzer.replace.size() + "\n" +
                        "updated XML = " + analyzer.updatedXML.get() + "\n" +
                        "updated BIN = " + analyzer.updatedBIN.get() + "\n" +
                        "skipped XML = " + analyzer.skippedXML.get() + "\n" +
                        "skipped BIN = " + analyzer.skippedBIN.get());

//                listener.info("analyzer.createCols:");
//                analyzer.createCols.forEach(listener::info);
//
//                listener.info("analyzer.createDocs:");
//                analyzer.createDocs.forEach(listener::info);

                listener.info("analyzer.deleteCols:");
                analyzer.deleteCols.forEach(listener::info);

                listener.info("analyzer.deleteDocs:");
                analyzer.deleteDocs.forEach(listener::info);

                listener.info("analyzer.moveCols:");
                analyzer.moveCols.entrySet().forEach(e -> listener.info(e.getKey() + " > " + e.getValue()));

                listener.info("analyzer.moveDocs:");
                analyzer.moveDocs.entrySet().forEach(e -> listener.info(e.getKey() + " > " + e.getValue()));

                listener.info("analyzer.replace:");
                analyzer.replace.forEach(listener::info);
            }

            listener.info(listener.warningsAndErrorsAsString());
        }
    }

    private ImportAnalyzer parseBackup(final RestoreListener listener, final DBBroker broker, final Stack<BackupDescriptor> descriptors, String uri, boolean disableTriggers) throws XMLDBException, FileNotFoundException, IOException, SAXException, ParserConfigurationException, URISyntaxException, AuthenticationException, ConfigurationException, PermissionDeniedException {

        if (disableTriggers) {
            broker.disableTriggers();
        }

        try {
            final SAXParserFactory saxFactory = SAXParserFactory.newInstance();
            saxFactory.setNamespaceAware(true);
            saxFactory.setValidating(false);
            final SAXParser sax = saxFactory.newSAXParser();
            final XMLReader reader = sax.getXMLReader();

            try {
                listener.restoreStarting();

                final ImportAnalyzer handler = new ImportAnalyzer(broker, listener, uri);

                while(!descriptors.isEmpty()) {
                    final BackupDescriptor descriptor = descriptors.pop();
                    final EXistInputSource is = descriptor.getInputSource();
                    is.setEncoding( "UTF-8" );


                    reader.setContentHandler(handler.handler(descriptor));
                    reader.parse(is);
                }

                return handler;

            } finally {
                listener.restoreFinished();
            }
        } finally {
            if (disableTriggers) {
                broker.enableTriggers();
            }
        }
    }

    private void restoreSystemCollection(final RestoreListener listener, final DBBroker broker, final Stack<BackupDescriptor> descriptors, String uri, boolean disableTriggers) throws XMLDBException, FileNotFoundException, IOException, SAXException, ParserConfigurationException, URISyntaxException, AuthenticationException, ConfigurationException, PermissionDeniedException {

        if (disableTriggers) {
            broker.disableTriggers();
        }

        try {
            final SAXParserFactory saxFactory = SAXParserFactory.newInstance();
            saxFactory.setNamespaceAware(true);
            saxFactory.setValidating(false);
            final SAXParser sax = saxFactory.newSAXParser();
            final XMLReader reader = sax.getXMLReader();

            try {
                listener.restoreStarting();

                while(!descriptors.isEmpty()) {
                    final BackupDescriptor descriptor = descriptors.pop();
                    final EXistInputSource is = descriptor.getInputSource();
                    is.setEncoding( "UTF-8" );

                    final SystemImportHandler handler = new SystemImportHandler(broker, listener, uri, descriptor);

                    reader.setContentHandler(handler);
                    reader.parse(is);
                }

            } finally {
                listener.restoreFinished();
            }
        } finally {
            if (disableTriggers) {
                broker.enableTriggers();
            }
        }
    }


    private void createPhase(final RestoreListener listener, final DBBroker broker, final Stack<BackupDescriptor> descriptors, String uri, boolean disableTriggers, ImportAnalyzer master) throws XMLDBException, IOException, SAXException, ParserConfigurationException, URISyntaxException, AuthenticationException, ConfigurationException, PermissionDeniedException {

        if (disableTriggers) {
            broker.disableTriggers();
        }

        try {
            final SAXParserFactory saxFactory = SAXParserFactory.newInstance();
            saxFactory.setNamespaceAware(true);
            saxFactory.setValidating(false);
            final SAXParser sax = saxFactory.newSAXParser();
            final XMLReader reader = sax.getXMLReader();

            try {
                listener.restoreStarting();


                while(!descriptors.isEmpty()) {
                    final BackupDescriptor descriptor = descriptors.pop();
                    final EXistInputSource is = descriptor.getInputSource();
                    is.setEncoding( "UTF-8" );

                    final ImportNewResourcesHandler handler = new ImportNewResourcesHandler(broker, listener, uri, descriptor, master);

                    reader.setContentHandler(handler);
                    reader.parse(is);
                }

            } finally {
                listener.restoreFinished();
            }
        } finally {
            if (disableTriggers) {
                broker.enableTriggers();
            }
        }
    }

    private void finalPhase(final RestoreListener listener, final DBBroker broker, final Stack<BackupDescriptor> descriptors, String uri, boolean disableTriggers, ImportAnalyzer master) throws XMLDBException, FileNotFoundException, IOException, SAXException, ParserConfigurationException, URISyntaxException, AuthenticationException, ConfigurationException, PermissionDeniedException {

        if (disableTriggers) {
            broker.disableTriggers();
        }

        try {
            final SAXParserFactory saxFactory = SAXParserFactory.newInstance();
            saxFactory.setNamespaceAware(true);
            saxFactory.setValidating(false);
            final SAXParser sax = saxFactory.newSAXParser();
            final XMLReader reader = sax.getXMLReader();

            try {
                listener.restoreStarting();


                while(!descriptors.isEmpty()) {
                    final BackupDescriptor descriptor = descriptors.pop();
                    final EXistInputSource is = descriptor.getInputSource();
                    is.setEncoding( "UTF-8" );

                    final ImportDiffsOnlyHandler handler = new ImportDiffsOnlyHandler(broker, listener, uri, descriptor, master);

                    reader.setContentHandler(handler);
                    reader.parse(is);
                }

            } finally {
                listener.restoreFinished();
            }
        } finally {
            if (disableTriggers) {
                broker.enableTriggers();
            }
        }
    }

    private static Stack<BackupDescriptor> getSystemBackupDescriptors(File contents) throws XMLDBException, IOException {

        final Stack<BackupDescriptor> descriptors = new Stack<>();

        final BackupDescriptor bd = getBackupDescriptor(contents);

        // check if the system collection is in the backup. This should be processed first
        final BackupDescriptor sysDescriptor = bd.getChildBackupDescriptor(XmldbURI.SYSTEM_COLLECTION_NAME);

        // check if the system/security collection is in the backup, this must be the first system collection processed
        if(sysDescriptor != null) {
            descriptors.push(sysDescriptor);

            final BackupDescriptor secDescriptor = sysDescriptor.getChildBackupDescriptor("security");
            if(secDescriptor != null) {
                descriptors.push(secDescriptor);
            }
        }

        return descriptors;
    }

    private static Stack<BackupDescriptor> getBackupDescriptors(File contents) throws XMLDBException, IOException {

        final Stack<BackupDescriptor> descriptors = new Stack<>();

        do {
            final BackupDescriptor bd = getBackupDescriptor(contents);
            descriptors.push(bd);

            contents = null;

            final Properties properties = bd.getProperties();
            if((properties != null ) && "yes".equals(properties.getProperty("incremental", "no"))) {
                final String previous = properties.getProperty("previous", "");

                if(previous.length() > 0) {
                    contents = new File(bd.getParentDir(), previous);

                    if(!contents.canRead()) {
                        throw new XMLDBException(ErrorCodes.PERMISSION_DENIED, "Required part of incremental backup not found: " + contents.getAbsolutePath());
                    }
                }
            }
        } while(contents != null);

        return descriptors;
    }
    
    private static BackupDescriptor getBackupDescriptor(File f) throws IOException {
        final BackupDescriptor bd;
        if(f.isDirectory()) {
            bd = new FileSystemBackupDescriptor(new File(new File(f, "db"), BackupDescriptor.COLLECTION_DESCRIPTOR));
        } else if(f.getName().toLowerCase().endsWith( ".zip" )) {
            bd = new ZipArchiveBackupDescriptor(f);
        } else {
            bd = new FileSystemBackupDescriptor(f);
        }
        return bd;
    }
    
    private void setAdminCredentials(DBBroker broker, String newCredentials) throws ConfigurationException, PermissionDeniedException {
        final Subject subject = broker.getSubject();
        subject.setPassword(newCredentials);
        subject.save(broker);
    }

    private void moveDocuments(RestoreListener listener, DBBroker broker, Map<String, String> map) throws PermissionDeniedException, TransactionException, LockException, IOException, TriggerException {
        Subject subject = broker.getSubject();

        broker.setSubject(broker.getDatabase().getSecurityManager().getSystemSubject());

        List<String> list = new ArrayList<>(map.keySet());

        try (Txn tx = broker.beginTx()) {

            for (String doc : list) {
                XmldbURI url = XmldbURI.create(doc);

                Collection c = broker.getCollection(url.removeLastSegment());
                if (c == null) {
                    listener.warn("No src collection during document move: '" + url + "'");
                    map.remove(doc);
                    continue;
                }

                DocumentImpl d = c.getDocument(broker, url.lastSegment());

                if (d == null) {
                    listener.warn("No document during document move: '" + url + "'");
                    map.remove(doc);
                    continue;
                }

                XmldbURI dst = XmldbURI.create(map.get(doc));

                Collection cDst = broker.getCollection(dst.removeLastSegment());
                if (cDst == null) {
                    listener.warn("No dst collection during document move: '" + dst + "'");
                    continue;
                }

                DocumentImpl dDst = cDst.getDocument(broker, dst.lastSegment());

                if (dDst != null) {
                    listener.warn("Dst collection have document during document move: '" + dst + "'");
                    continue;
                }

                broker.moveResource(tx, d, cDst, dst.lastSegment());

                map.remove(doc);
            }

            tx.success();

        } finally {
            broker.setSubject(subject);
        }
    }

    private void moveCollections(RestoreListener listener, DBBroker broker, ImportAnalyzer analyzer) throws PermissionDeniedException, TransactionException, LockException, IOException, TriggerException {
        Subject subject = broker.getSubject();

        broker.setSubject(broker.getDatabase().getSecurityManager().getSystemSubject());

        List<String> list = new ArrayList<>(analyzer.moveCols.keySet());

        try (Txn tx = broker.beginTx()) {

            for (String col : list) {
                XmldbURI url = XmldbURI.create(col);

                Collection c = broker.getCollection(url);
                if (c == null) {
                    listener.warn("No src collection during collection move: '" + url + "'");
                    analyzer.moveCols.remove(col);
                    continue;
                }

                Set<String> affected = new HashSet<>();

                if (canMove(broker, c, analyzer, affected)) {

                    XmldbURI dst = XmldbURI.create(analyzer.moveCols.get(col));

                    Collection cDst = broker.getCollection(dst);
                    if (cDst == null) {
                        listener.warn("No dst collection during collection move: '" + dst + "'");
                        continue;
                    }

                    broker.moveCollection(tx, c, cDst, dst.lastSegment());

                    for (String u : affected) {
                        analyzer.moveCols.remove(u);
                        analyzer.moveDocs.remove(u);
                    }
                }
            }

            tx.success();

        } finally {
            broker.setSubject(subject);
        }
    }

    private boolean canMove(DBBroker broker, Collection col, ImportAnalyzer analyzer, Set<String> affected) throws PermissionDeniedException {

        affected.add(col.getURI().toString());

        if (col.isEmpty(broker)) {
            return true;
        }

        Iterator<DocumentImpl> docs = col.iterator(broker);
        while (docs.hasNext()) {
            DocumentImpl doc = docs.next();

            String url = doc.getURI().toString();

            if (!analyzer.deleteDocs.contains(url)) {
                return false;
            }
            affected.add(url);
        }

        XmldbURI path = col.getURI();
        Iterator<XmldbURI> cols = col.collectionIterator(broker);
        while (cols.hasNext()) {
            XmldbURI url = path.appendInternal(cols.next());

            if (!analyzer.deleteCols.contains(url.toString())) {
                return false;
            }

            Collection c = broker.getCollection(url);
            if (!canMove(broker, c, analyzer, affected)) {
                return false;
            }
        }

        return true;
    }

    private void deleteDocuments(RestoreListener listener, DBBroker broker, Set<String> docs) throws PermissionDeniedException, TransactionException, LockException, IOException, TriggerException {

        //disable trash manager
        TrashManager trashManager = db.getTrashManager();
        db.setTrashManager(null);

        Subject subject = broker.getSubject();

        broker.setSubject(broker.getDatabase().getSecurityManager().getSystemSubject());

        List<String> list = new ArrayList<>(docs);

        try (Txn tx = broker.beginTx()) {

            for (String doc : list) {
                XmldbURI url = XmldbURI.create(doc);

                Collection c = broker.getCollection(url.removeLastSegment());
                if (c == null) {
                    listener.warn("No collection during document remove: '" + url + "'");
                    docs.remove(doc);
                    continue;
                }

                DocumentImpl d = c.getDocument(broker, url.lastSegment());

                if (d == null) {
                    listener.warn("No document during document remove: '" + url + "'");
                    docs.remove(doc);
                    continue;
                }

                c.removeResource(tx, broker, d);
                docs.remove(doc);
            }

            tx.success();

        } finally {
            db.setTrashManager(trashManager);
            broker.setSubject(subject);
        }
    }

    private void deleteEmptyCollections(RestoreListener listener, DBBroker broker, Set<String> cols) throws PermissionDeniedException, TransactionException, LockException, IOException, TriggerException {
        //disable trash manager
        TrashManager trashManager = db.getTrashManager();
        db.setTrashManager(null);

        Subject subject = broker.getSubject();

        broker.setSubject(broker.getDatabase().getSecurityManager().getSystemSubject());

        List<String> list = new ArrayList<>(cols);

        try (Txn tx = broker.beginTx()) {

            for (int i = list.size() - 1; i >= 0; i--) {
                String col = list.get(i);
                XmldbURI url = XmldbURI.create(col);

                Collection c = broker.getCollection(url);
                if (c == null) {
                    listener.warn("No collection during collection remove: '" + url + "'");
                    cols.remove(col);
                    continue;
                }

                if (c.isEmpty(broker)) {
                    broker.removeCollection(tx, c);

                    cols.remove(col);

//                } else {
//                    String msg = "Collection none empty during collection remove: '" + url + "'";
//                    listener.error(msg);
//
//                    throw new IOException(msg);
                }
            }

            tx.success();

        } finally {
            db.setTrashManager(trashManager);
            broker.setSubject(subject);
        }
    }
}