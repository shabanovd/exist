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
package org.exist.revisions;

import static org.exist.dom.DocumentImpl.BINARY_FILE;
import static org.exist.dom.DocumentImpl.XML_FILE;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.IOUtils;
import org.exist.Database;
import org.exist.Namespaces;
import org.exist.Resource;
import org.exist.ResourceMetadata;
import org.exist.collections.Collection;
import org.exist.collections.triggers.TriggerException;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.dom.QName;
import org.exist.plugin.PluginsManager;
import org.exist.security.ACLPermission;
import org.exist.security.Permission;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.DBBroker;
import org.exist.storage.MetaStreamListener;
import org.exist.storage.lock.Lock;
import org.exist.storage.md.MetaData;
import org.exist.storage.md.Metas;
import org.exist.storage.serializers.Serializer;
import org.exist.storage.txn.TransactionException;
import org.exist.storage.txn.Txn;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

//import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class RCSManager implements Constants {
    
    final static String XML_DATA = "data.xml";
    final static String BIN_DATA = "data.bin";
    
    final static int UNKNOWN = -1;
    final static int EQ = 0;
    final static int DIFF = 1;
    
    private static RCSManager instance = null;
    
    public static RCSManager get() {
        return instance;
    }
    
    Database db;
    
    MetaData md;
    
    Path rcFolder;
    Path uuidFolder;
    Path commitLogsFolder;
    Path snapshotLogsFolder;
    
    public RCSManager(PluginsManager manager) throws PermissionDeniedException, IOException {
        db = manager.getDatabase();

        md = MetaData.get();
        
        Path dbData = FileSystems.getDefault().getPath(db.getActiveBroker().getDataFolder().getAbsolutePath());
        rcFolder = dbData.resolve("RCS");
        
        uuidFolder = rcFolder.resolve("uuids");
        commitLogsFolder = rcFolder.resolve("commits");
        snapshotLogsFolder = rcFolder.resolve("snapshots");
        
        Files.createDirectories(commitLogsFolder);
        Files.createDirectories(snapshotLogsFolder);
        
        instance = this;
    }
    
    public RCSResource resource(String uuid) {
        
        Path location = resourceFolder(uuid, uuidFolder);
        
        if (Files.notExists(location)) return null;
        
        return new RCSResource(location);
    }
    
    public void snapshot(Collection collection, Handler h) throws PermissionDeniedException, LockException, XMLStreamException, IOException {

        DBBroker broker = db.getActiveBroker();
        
        Path logPath = logPath(snapshotLogsFolder);
        String snapshotId = logPath.getFileName().toString();
        
        try (BufferedWriter commitLogStream = Files.newBufferedWriter(logPath, ENCODING)) {
            XMLOutputFactory xof = XMLOutputFactory.newInstance();
            
            XMLStreamWriter log = xof.createXMLStreamWriter(commitLogStream);
            
            //XMLStreamWriter log = new IndentingXMLStreamWriter(xof.createXMLStreamWriter(commitLogStream));
            
            log.writeStartDocument();
            
            log.writeStartElement("RCS", "snapshot-log", "http://exist-db.org/RCS");
            log.writeDefaultNamespace(Namespaces.EXIST_NS);
            //writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);
            
            log.writeAttribute("id", snapshotId);

            process(broker, log, collection, logPath, h);
            
            log.writeEndElement();
            log.writeEndDocument();
        }
    }

    private void process(DBBroker broker, XMLStreamWriter log, Collection collection, Path logPath, Handler h) 
        throws PermissionDeniedException, LockException, XMLStreamException {
        
        List<Collection> toProcess = new ArrayList<Collection>();

        collection.getLock().acquire(Lock.READ_LOCK);
        try {
            toProcess.add(collection);
            
            while (!toProcess.isEmpty()) {
                
                List<Collection> nexts = new ArrayList<Collection>();
    
                for (Collection col : toProcess) {
            
                    Iterator<DocumentImpl> docs = col.iteratorNoLock(broker);
                    
                    while (docs.hasNext()) {
                        DocumentImpl doc = docs.next();
                        
                        processEntry(doc, broker, log, logPath, h);
                    }
                    
                    List<Collection> next = new ArrayList<Collection>(col.countSubCollection());
        
                    Iterator<XmldbURI> cols = col.collectionIteratorNoLock(broker);
                    while (cols.hasNext()) {
                        XmldbURI childName = cols.next();
                        
                        XmldbURI uri = col.getURI().append(childName);
                        
                        Collection childColl = broker.getCollection(uri);
                        
                        if (childColl != null) {
                            
                            childColl.getLock().acquire(Lock.READ_LOCK);
                            next.add(childColl);
                            
                            log.writeStartElement("entry");
                            log.writeAttribute("uri", uri.toString());
                
                            try {
                                Path folder = makeRevision(broker, null, uri, childColl, logPath, h);
                                
                                if (folder != null) {
                                    log.writeAttribute("path", rcFolder.relativize(folder).toString());
                                }
                                
                            } catch (Exception e) {
                                h.error(uri, e);

                                log.writeAttribute("error", e.getMessage());
                            }
                            
                            log.writeEndElement();
                        }
                    }
                    nexts.addAll(next);
                }
                
                //release lock
                for (Collection col : toProcess) {
                    col.getLock().release(Lock.READ_LOCK);
                }
                
                toProcess = nexts;
            }
        } finally {
            //just make sure that lock release
            for (Collection col : toProcess) {
                col.getLock().release(Lock.READ_LOCK);
            }
        }
    }
    
    private void processEntry(DocumentImpl doc, DBBroker broker, XMLStreamWriter log, Path logPath, Handler h) throws XMLStreamException {
        XmldbURI uri = doc.getURI();
        
        log.writeStartElement("entry");
        log.writeAttribute("uri", uri.toString());
        
        try {
            
            doc.getUpdateLock().acquire(Lock.READ_LOCK);

            Path folder = null;
            try {
                folder = makeRevision(broker, null, uri, doc, logPath, h);
            } finally {
                doc.getUpdateLock().release(Lock.READ_LOCK);
            }
            
            if (folder != null) {
                log.writeAttribute("path", rcFolder.relativize(folder).toString());
            }
            
        } catch (Exception e) {
            h.error(doc.getURI(), e);
            
            log.writeAttribute("error", e.getMessage());
        }

        log.writeEndElement();
        
    }

    public void commit(String msg, Sequence urls, Handler h) throws IOException, XMLStreamException, XPathException {
        
        DBBroker broker = db.getActiveBroker();
        
        Path logPath = logPath(commitLogsFolder);
        
        String commitId = logPath.getFileName().toString();
        
        try (BufferedWriter commitLogStream = Files.newBufferedWriter(logPath, ENCODING)) {
            XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
            
            XMLStreamWriter log = outputFactory.createXMLStreamWriter(commitLogStream);
        
            log.writeStartDocument();
            
            log.writeStartElement("RCS", "commit-log", "http://exist-db.org/RCS");
            log.writeDefaultNamespace(Namespaces.EXIST_NS);
            //writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);
            
            log.writeAttribute("id", commitId);

            log.writeStartElement("message");
            log.writeCData(msg);
            log.writeEndElement();
    
            for (SequenceIterator i = urls.iterate(); i.hasNext();) {
                
                Item item = i.nextItem();
                if (item == null) continue;
                
                String url = item.getStringValue();
                
                XmldbURI uri = XmldbURI.create(url);
                
                //processEntry(doc, broker, serializer, log, logPath, h);
                
                log.writeStartElement("entry");
                log.writeAttribute("uri", uri.toString());
    
                try {
                    Path folder = makeRevision(broker, uri, logPath, h);
                    
                    if (folder != null) {
                        log.writeAttribute("path", rcFolder.relativize(folder).toString());
                    }
                    
                } catch (Exception e) {
                    h.error(uri, e);

                    log.writeAttribute("error", e.getMessage());
                }
                
                log.writeEndElement();
            }
        
            log.writeEndDocument();
        }
    }
    
    private synchronized Path logPath(Path location) throws IOException {
        for (int i = 0; i < 10; i++) {
            Path path = location.resolve( String.valueOf( System.currentTimeMillis() ) );
            
            if (!Files.exists(path)) return path;
        }
        
        throw new IOException("can't create commit log");
    }

    private Serializer serializer(DBBroker broker) {
        Serializer serializer = broker.getSerializer();
        serializer.setUser(broker.getSubject());
        try {
            serializer.setProperty("omit-xml-declaration", "no");
        } catch (Exception e) {}
        
        return serializer;
    }
    
    private Path makeRevision(DBBroker broker, XmldbURI uri, Path logPath, Handler h) 
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {

        String uuid = null;
        if ((uuid = uuid(uri, h)) == null) return null;
        
        Path folder = null;
        
        DocumentImpl doc = broker.getXMLResource(uri, Lock.READ_LOCK);
        if (doc == null) {
            Collection col = broker.getCollection(uri);
            
            if (col == null) {
                h.error(uri, "not found");
                return null;
            }
            
            makeRevision(broker, uuid, uri, col, logPath, h);

        } else {

            try {
                makeRevision(broker, uuid, uri, doc, logPath, h);

            } finally {
                doc.getUpdateLock().release(Lock.READ_LOCK);
            }
        }
        
        return folder;
    }
    
    private void linkLog(Path folder, Path logPath) throws IOException {
        if (folder != null) {
            Files.createSymbolicLink(folder.resolve("log"), folder.relativize(logPath));
        }
    }
    
    private String uuid(XmldbURI uri, Handler h) {
        Metas metas = md.getMetas(uri);
        if (metas == null) {
            h.error(uri, "missing metas");
            return null;
        }

        String uuid = metas.getUUID();
        if (uuid == null) {
            h.error(uri, "missing uuid");
            return null;
        }
        
        return uuid;
    }
    
    private Path makeRevision(DBBroker broker, String uuid, XmldbURI uri, Collection col, Path logPath, Handler h) 
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {
        
        if (uuid == null) {
            if ((uuid = uuid(uri, h)) == null) return null;
        }

        Path folder = revFolder(uuid, uuidFolder);
        
        if (folder == null) {
            h.error(uri, "can't create revision folder");
            return null;
        }

        Files.createDirectories(folder);

        processMetas(uuid, col, folder, h);
        
        linkLog(folder, logPath);
        
        h.processed(uri);
        
        return folder;
    }

    private Path makeRevision(DBBroker broker, String uuid, XmldbURI uri, DocumentImpl doc, Path logPath, Handler h) 
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {
        
        if (uuid == null) {
            if ((uuid = uuid(uri, h)) == null) return null;
        }

        Path folder = revFolder(uuid, uuidFolder);
        
        if (folder == null) {
            h.error(uri, "can't create revision folder");
            return null;
        }
        Files.createDirectories(folder);

        MessageDigest digest = messageDigest();

        Path dataFile;
        
        switch (doc.getResourceType()) {
        case XML_FILE:
            
            dataFile = folder.resolve(XML_DATA);

            try (OutputStream fileStream = Files.newOutputStream(dataFile)) {
                
                Writer writerStream = new OutputStreamWriter(
                    new DigestOutputStream(fileStream, digest), 
                    ENCODING.newEncoder()
                );
                
                try (Writer writer = new BufferedWriter(writerStream)) {
                    serializer(broker).serialize(doc, writer);
                }
            }
            
            break;

        case BINARY_FILE:
            
            dataFile = folder.resolve(BIN_DATA);

            try (InputStream is = broker.getBinaryResource((BinaryDocument)doc)) {
                try (OutputStream fileStream = Files.newOutputStream(dataFile)) {
                
                    DigestOutputStream stream = new DigestOutputStream(fileStream, digest);
                    
                    IOUtils.copy(is, stream);
                }                            
            }

            break;
        
        default:
            h.error(uri, "unknown type");
            return null;
        }
        
        writeDigest(dataFile, digest);

        processMetas(uuid, doc, folder, h);
        
        linkLog(folder, logPath);
        
        h.processed(uri);
        
        return folder;
    }
    
    private Path resourceFolder(String docId, Path folder) {
        
        return folder
                .resolve(docId.substring(0, 4))
                .resolve(docId.substring(4, 8))
                .resolve(docId);
    }

    private Path revFolder(String docId, Path folder) throws IOException {
        
        Path location = resourceFolder(docId, folder);
        
        long nextId = revNextId(location);
        
        for (int i = 0; i < 5; i++) {
            Path dir = location.resolve(String.valueOf(nextId));
            if (!Files.exists(dir)) {
                return dir;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        
        throw new IOException("can't get new id for revision");
    }

    private long revNextId(Path fileFolder) {
        return System.currentTimeMillis();
    }

    private void processMetas(String uuid, Resource resource, Path folder, Handler h) throws XMLStreamException, IOException {
        Path file = folder.resolve(FILE_META);
        
        MessageDigest digest = messageDigest();

        try (OutputStream metaFile = Files.newOutputStream(file)) {
            
            if (digest == null) {
                processMetas(uuid, resource, metaFile, h);
                
            } else {
                try (OutputStream stream = new DigestOutputStream(metaFile, digest)) {
                    processMetas(uuid, resource, stream, h);
                }
            }
        }

        writeDigest(file, digest);
    }
    
    private Path digestPath(Path file) {
        return file.getParent().resolve(file.getFileName()+".sha1");
    }
    
    private byte[] digestHex(MessageDigest digest) {
        return Hex.encodeHexString( digest.digest() ).getBytes();
    }

    private void writeDigest(Path file, MessageDigest digest) throws IOException {
        if (digest == null) return;
        
        Files.write( digestPath(file), digestHex(digest) );
    }
    
    private MessageDigest messageDigest() throws IOException {
        try {
            return MessageDigest.getInstance(MessageDigestAlgorithms.SHA_1);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }
    
    private void processMetas(String uuid, Resource resource, OutputStream stream, Handler h) throws XMLStreamException, IOException {
        
        
        XmldbURI uri = resource.getURI();
        String url = uri.toString();
        String name = uri.lastSegment().toString();
            
        ResourceMetadata metadata = resource.getMetadata();
        
        String mimeType = metadata.getMimeType();
        
        long createdTime = metadata.getCreated();
        long lastModified = metadata.getLastModified();
        
        Permission perm = resource.getPermissions();

        String parentUuid = null;
        XmldbURI parentUri = uri.removeLastSegment();

        if (!(uri.equalsInternal(XmldbURI.DB) 
                || parentUri.equalsInternal(XmldbURI.DB) 
                || parentUri.equalsInternal(XmldbURI.EMPTY_URI)
            )
        ) {
            parentUuid = md.URItoUUID(parentUri);
            if (parentUuid == null) {
                h.error(uri, "missing parent's uuid");
            }
        }

        GregorianCalendar date = new GregorianCalendar(GMT);

        XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        
        final XMLStreamWriter writer = outputFactory.createXMLStreamWriter(stream);
        
        writer.writeStartDocument();
        writer.writeStartElement("RCS", "metas", "http://exist-db.org/RCS");
        writer.writeDefaultNamespace(Namespaces.EXIST_NS);
        writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);
        
        write(writer, EL_FILE_NAME, name);
        write(writer, EL_FILE_PATH, url);
        
        if (parentUuid != null) {
            write(writer, PARENT_UUID, parentUuid);
        }

        write(writer, EL_META_TYPE, mimeType);
        
        date.setTimeInMillis(createdTime);
        write(writer, EL_CREATED, DatatypeConverter.printDateTime(date));
        
        date.setTimeInMillis(lastModified);
        write(writer, EL_LAST_MODIFIED, DatatypeConverter.printDateTime(date));
        

        writer.writeStartElement(EL_PERMISSION);
        writeUnixStylePermissionAttributes(writer, perm);
        if(perm instanceof ACLPermission) {
            writeACLPermission(writer, (ACLPermission)perm);
        }
        writer.writeEndElement();

        
        writer.writeStartElement(MetaData.PREFIX, EL_METASTORAGE, MetaData.NAMESPACE_URI);
        writer.writeAttribute(MetaData.PREFIX, MetaData.NAMESPACE_URI, EL_UUID, uuid);

        md.streamMetas(uri, new MetaStreamListener() {

            @Override
            public void metadata(QName key, Object value) {
                try {
                    writer.writeStartElement(key.getPrefix(), key.getLocalName(), key.getNamespaceURI());
                    
                    if (value instanceof String) {
                        writer.writeCharacters(value.toString());
                    
                    } else {
                        //XXX: log?
                    }
                    writer.writeEndElement();
                } catch (XMLStreamException e) {
                    e.printStackTrace();
                }
            }
            
        });
        writer.writeEndElement();

        writer.writeEndElement();
        writer.writeEndDocument();
        
        writer.flush();
        writer.close();
    }
    
    private static void writeUnixStylePermissionAttributes(XMLStreamWriter writer, Permission permission) throws XMLStreamException {
        if (permission == null) return;

        writer.writeAttribute(AT_OWNER, permission.getOwner().getName());
        writer.writeAttribute(AT_GROUP, permission.getGroup().getName());
        writer.writeAttribute(AT_MODE, Integer.toOctalString(permission.getMode()));
    }
    
    private static void writeACLPermission(XMLStreamWriter writer, ACLPermission acl) throws XMLStreamException {
        if (acl == null) return;
        
        writer.writeStartElement(EL_ACL);
        
        writer.writeAttribute(AT_VERSION, Short.toString(acl.getVersion()));

        for(int i = 0; i < acl.getACECount(); i++) {
            writer.writeStartElement(EL_ACE);
            
            writer.writeAttribute(AT_INDEX, Integer.toString(i));
            writer.writeAttribute(AT_TARGET, acl.getACETarget(i).name());
            writer.writeAttribute(AT_WHO, acl.getACEWho(i));
            writer.writeAttribute(AT_ACCESS_TYPE, acl.getACEAccessType(i).name());
            writer.writeAttribute(AT_MODE, Integer.toOctalString(acl.getACEMode(i)));

            writer.writeEndElement();
        }
        
        writer.writeEndElement();
    }

    private void write(XMLStreamWriter w, String name, String value) throws XMLStreamException {
        w.writeStartElement(name);
        //writer.writeAttribute(name, value);
        w.writeCharacters(value);
        w.writeEndElement();
    }

    public void restore(Path location, Handler h) throws IOException {
        
        DBBroker broker = db.getActiveBroker();
        
        try (DirectoryStream<Path> firstDirs = Files.newDirectoryStream(location)) {
            for (Path firstDir : firstDirs) {

                try (DirectoryStream<Path> secondDirs = Files.newDirectoryStream(firstDir)) {
                    for (Path secondDir : secondDirs) {

                        try (DirectoryStream<Path> thirdDirs = Files.newDirectoryStream(secondDir)) {
                            
                            for (Path thirdDir : thirdDirs) {
                                
                                restoreResource(broker, thirdDir, h);
                            }
                        }
                    }
                }
            }            
        }
    }
    
    private int checkHash(DBBroker broker, Path location, DocumentImpl doc) throws IOException, SAXException {
        
        Path file;
        MessageDigest digest = messageDigest();
        
        OutputStream out = new FakeOutputStream();
        DigestOutputStream digestStream = new DigestOutputStream(out, digest);
        
        switch (doc.getResourceType()) {
        case XML_FILE:
            
            file = location.resolve(XML_DATA);
            
            Writer writerStream = new OutputStreamWriter(
                digestStream, 
                ENCODING.newEncoder()
            );
            
            Serializer serializer = serializer(broker);
            
            try (Writer writer = new BufferedWriter(writerStream)) {
                serializer.serialize(doc, writer);
            }
            
            break;

        case BINARY_FILE:
            
            file = location.resolve(BIN_DATA);

            try (InputStream is = broker.getBinaryResource((BinaryDocument)doc)) {
                IOUtils.copy(is, digestStream);
            }

            break;
        
        default:
            //h.error(uri, "unknown type");
            return UNKNOWN;
        }
        
        //XXX: make safer
        
        byte[] revHash = Files.readAllBytes(digestPath(file));
        
        byte[] calcHash = digestHex(digest);
        
        if (Arrays.equals(revHash, calcHash)) {
            return EQ;
        }
        
        return DIFF;
    }
    
    private void restoreResource(DBBroker broker, Path location, Handler h) throws IOException {
        Path rev = lastRev(location);
        if (rev == null) {
            //h.error(location, "no revisions");
            return;
        }
            
        try {
            Path meta = rev.resolve(FILE_META);
            
            SAXParserFactory parserFactor = SAXParserFactory.newInstance();
            SAXParser parser = parserFactor.newSAXParser();
            
            MetasHandler dh = new MetasHandler();
            
            try (InputStream metaStream = Files.newInputStream(meta)) {
                parser.parse(metaStream, dh);
            }
            
            Collection parent = null;
            DocumentImpl doc = null;
            
            Collection col = broker.getCollection(dh.uri);
            if (col == null) {
                parent = broker.getCollection(dh.uri.removeLastSegment());
                
                if (parent != null) {
                    doc = parent.getDocument(broker, dh.uri.lastSegment());
                }
            }

            if ("collection".equals( dh.mimeType ) ) {
                
                remove(broker, parent, doc);
                
                if (col == null) {
                    createCollection(broker, parent, dh);
                }

                restoreMetas(broker, col, meta, dh);
                
                return;
                
            } else {
                
                if (doc != null) {
                    switch (checkHash(broker, rev, doc)) {
                    case EQ:
                        restoreMetas(broker, doc, meta, dh);
                        return;
                    case UNKNOWN:
                        restoreMetas(broker, doc, meta, dh);
                        return;
                    }
                }
                
                //delete
                remove(broker, parent, doc);
                
                createDocument(broker, parent, dh);
                
                restoreMetas(broker, doc, meta, dh);
            }
            
        } catch (Exception e) {
            h.error(location, e);
            return;
        }
    }
    
    private void createCollection(DBBroker broker, Collection parent, MetasHandler dh) {
        //XXX: code
        System.out.println("create collection "+dh.uri);
        
        try (Txn tx = broker.beginTx()) {
            
            if (parent == null) {
                
            }
        }
    }
    
    private void createDocument(DBBroker broker, Collection parent, MetasHandler dh) {
        //XXX: code
        System.out.println("create document "+dh.uri);
        
//        try (Txn tx = broker.beginTx()) {
//            
//        }
    }
    
    private void remove(DBBroker broker, Collection docCol, DocumentImpl doc) throws TriggerException, PermissionDeniedException, LockException, TransactionException {
        if (doc == null) return;

        try (Txn tx = broker.beginTx()) {
            
            docCol.removeResource(tx, broker, doc);
            
            tx.success();
        }
    }
    
    private void restoreMetas(DBBroker broker, Resource resource, Path meta, MetasHandler dh) {
        // TODO Auto-generated method stub
        
    }

    protected Path lastRev(Path location) throws IOException {
        Path revFolder = null;
        long last = 0;
        try (DirectoryStream<Path> revs = Files.newDirectoryStream(location)) {
            
            for (Path rev : revs) {
                try {
                    long cur = Long.parseLong( rev.getFileName().toString() );
                    if (cur > last) {
                        last = cur;
                        revFolder = rev;
                    }
                } catch (NumberFormatException e) {}
            }
        }
        
        return revFolder;
    }
    
    class MetasHandler extends DefaultHandler {
        
        String parentUuid;
        
        XmldbURI uri;
        String name;
        String mimeType;

        long createdTime;
        long lastModified;
        
        //Permission perm;

        String content = null;
        
        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            
            if (content == null) return;
            
            switch(qName){
            case EL_FILE_NAME:
                this.name = content;
                break;
            case EL_FILE_PATH:
                this.uri = XmldbURI.create(content);
                break;
            case PARENT_UUID:
                parentUuid = content;
                break;
            case EL_META_TYPE:
                mimeType = content;
                break;
            case EL_CREATED:
                createdTime = DatatypeConverter.parseDateTime(content).getTimeInMillis();
                break;
            case EL_LAST_MODIFIED:
                lastModified = DatatypeConverter.parseDateTime(content).getTimeInMillis();
                break;
            }
            
            content = null;
        }
        
        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            content = String.copyValueOf(ch, start, length).trim();
            
            super.characters(ch, start, length);
        }
    }
    
    
    class FakeOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {}
        
        @Override
        public void write(byte b[]) throws IOException {}

        @Override
        public void write(byte b[], int off, int len) throws IOException {}

        @Override
        public void flush() throws IOException {}

        @Override
        public void close() throws IOException {}
    }
}
