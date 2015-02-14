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
import static org.exist.revisions.Utils.*;

import static java.nio.file.Files.createDirectories;
import static org.exist.storage.lock.Lock.READ_LOCK;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.exist.*;
import org.exist.EventListener;
import org.exist.backup.BackupHandler;
import org.exist.backup.RestoreHandler;
import org.exist.collections.Collection;
import org.exist.collections.IndexInfo;
import org.exist.collections.triggers.TriggerException;
import org.exist.dom.*;
import org.exist.plugin.PluginsManager;
import org.exist.security.*;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.storage.md.MetaData;
import org.exist.storage.md.Metas;
import org.exist.storage.serializers.Serializer;
import org.exist.storage.txn.TransactionException;
import org.exist.storage.txn.Txn;
import org.exist.util.EXistInputSource;
import org.exist.util.FileInputSource;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

//import com.sun.xml.internal.txw2.output.IndentingXMLStreamWriter;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class RCSManager implements Constants {

    protected final static Logger LOG = Logger.getLogger( RCSManager.class );
    
    final static int UNKNOWN = -1;
    final static int EQ = 0;
    final static int DIFF = 1;
    
    private static RCSManager instance = null;
    
    public static RCSManager get() {
        return instance;
    }
    
    Database db;

    PluginsManager manager;

    MetaData md;

    HashStorage hashStorage;
    
    Path rcFolder;

    Path uuidFolder;
    Path hashesFolder;

    Path commitLogsFolder;
    Path snapshotLogsFolder;

    Path tmpFolder;

    boolean noDots = false;

    List<EventListener<CommitLog>> commitsListener = new ArrayList<>();

    public RCSManager(PluginsManager manager) throws PermissionDeniedException, IOException {

        this.manager = manager;

        db = manager.getDatabase();

        md = MetaData.get();
        
        Path dbData = FileSystems.getDefault().getPath(db.getActiveBroker().getDataFolder().getAbsolutePath());

        rcFolder = dbData.resolve("RCS");
        
        uuidFolder          = folder("uuids");
        hashesFolder        = folder("hashes");
        commitLogsFolder    = folder("commits");
        snapshotLogsFolder  = folder("snapshots");
        tmpFolder           = folder("tmp");

        try  {
            tmpFolder.resolve(":");
        } catch (InvalidPathException e) {
            noDots = true;
        }

        //clean up tmp folder
        FileUtils.cleanDirectory(tmpFolder.toFile());
        
        instance = this;

        hashStorage = new HashStorage(hashesFolder, tmpFolder);
    }

    public HashStorage hashStorage() {
        return hashStorage;
    }

    public boolean registerCommitsListener(EventListener<CommitLog> listener) {
        return commitsListener.add(listener);
    }

    public boolean unregisterCommitsListener(EventListener<CommitLog> listener) {
        return commitsListener.remove(listener);
    }

    public Iterable<CommitReader> commits() {
        return new Commits(this);
    }

    private Path folder(String name) throws IOException {
        return createDirectories(rcFolder.resolve(name));
    }
    
    public RCSResource resource(String uuid) {
        
        Path location = resourceFolder(uuid, uuidFolder);
        
        if (Files.notExists(location)) return null;
        
        return new RCSResource(location);
    }
    
    public void snapshot(Collection collection, Handler h) throws PermissionDeniedException, LockException, XMLStreamException, IOException {

        DBBroker broker = db.getActiveBroker();

        BackupHandler bh = broker.getDatabase().getPluginsManager().getBackupHandler(LOG); //TODO: use Handler for logs!
        
        Path logPath = logPath(snapshotLogsFolder);
        String snapshotId = logPath.getFileName().toString();

        Path logRelativePath = rcFolder.relativize(logPath);
        
        try (BufferedWriter commitLogStream = Files.newBufferedWriter(logPath, ENCODING)) {
            XMLOutputFactory xof = XMLOutputFactory.newInstance();
            
            XMLStreamWriter log = xof.createXMLStreamWriter(commitLogStream);
            
            //XMLStreamWriter log = new IndentingXMLStreamWriter(xof.createXMLStreamWriter(commitLogStream));
            
            log.writeStartDocument();
            
            log.writeStartElement("RCS", "snapshot-log", "http://exist-db.org/RCS");
            log.writeDefaultNamespace(Namespaces.EXIST_NS);
            //writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);
            
            log.writeAttribute("id", snapshotId);

            process(broker, log, collection, logRelativePath, bh, h);
            
            log.writeEndElement();
            log.writeEndDocument();
        }
    }

    public CommitWriter commit(Handler handler) {
        return new CommitLog(this, handler);
    }

    private void process(DBBroker broker, XMLStreamWriter log, Collection collection, Path logPath, BackupHandler bh, Handler h)
        throws PermissionDeniedException, LockException, XMLStreamException {
        
        List<Collection> toProcess = new ArrayList<>();

        collection.getLock().acquire(Lock.READ_LOCK);
        try {
            toProcess.add(collection);
            
            while (!toProcess.isEmpty()) {
                
                List<Collection> nexts = new ArrayList<>();
    
                for (Collection col : toProcess) {
            
                    Iterator<DocumentImpl> docs = col.iteratorNoLock(broker);
                    
                    while (docs.hasNext()) {
                        DocumentImpl doc = docs.next();
                        
                        processEntry(doc, broker, log, logPath, bh, h);
                    }
                    
                    List<Collection> next = new ArrayList<>(col.countSubCollection());
        
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
                                Path folder = makeRevision(broker, null, uri, childColl, logPath, bh, h);
                                
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
    
    private void processEntry(DocumentImpl doc, DBBroker broker, XMLStreamWriter log, Path logPath, BackupHandler bh, Handler h) throws XMLStreamException {
        XmldbURI uri = doc.getURI();
        
        log.writeStartElement("entry");
        log.writeAttribute("uri", uri.toString());
        
        try {

            doc.getUpdateLock().acquire(Lock.READ_LOCK);

            Path folder = null;
            try {
                folder = makeRevision(broker, null, uri, doc, logPath, bh, h);
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

    protected void commit(CommitLog commitLog) throws IOException, XMLStreamException {
        
        DBBroker broker = db.getActiveBroker();

        BackupHandler bh = broker.getDatabase().getPluginsManager().getBackupHandler(LOG); //TODO: use Handler for logging!

        Path logPath = logPath(commitLogsFolder);
        
        String commitId = logPath.getFileName().toString();

        commitLog.id = commitId;

        Path logRelativePath = rcFolder.relativize(logPath);
        
        try (BufferedWriter commitLogStream = Files.newBufferedWriter(logPath, ENCODING)) {
            XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
            
            XMLStreamWriter log = outputFactory.createXMLStreamWriter(commitLogStream);
        
            log.writeStartDocument();
            
            log.writeStartElement("RCS", "commit-log", "http://exist-db.org/RCS");
            log.writeDefaultNamespace(Namespaces.EXIST_NS);
            //writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);
            
            log.writeAttribute("id", commitId);

            log.writeStartElement("author");
            log.writeCData(commitLog.author());
            log.writeEndElement();

            log.writeStartElement("message");
            log.writeCData(commitLog.message());
            log.writeEndElement();
    
            for (Change action : commitLog.acts) {
                
                //processEntry(doc, broker, serializer, log, logPath, h);
                
                log.writeStartElement("entry");

                try {
                    Path folder = makeRevision(broker, action, logRelativePath, bh, commitLog.handler);

                    if (folder != null) {
                        log.writeAttribute("uri", action.uri().toString());
                        log.writeAttribute("path", rcFolder.relativize(folder).toString());
                    }
                    
                } catch (Exception e) {
                    commitLog.handler.error(action.uri(), e);

                    log.writeAttribute("error", e.getMessage());
                }
                
                log.writeEndElement();
            }
        
            log.writeEndDocument();
        }

        for (EventListener<CommitLog> listener : commitsListener) {
            try {
                listener.onEvent(commitLog);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private synchronized Path logPath(Path location) throws IOException {
        GregorianCalendar date = new GregorianCalendar(GMT);

        long ts = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {

            date.setTimeInMillis(ts);
            String str = DatatypeConverter.printDateTime(date);

            if (noDots) str = str.replace(':','_');

            Path path = location.resolve( str.substring(0, 7) ).resolve( str );

            if (!Files.exists(path)) {

                if (Files.notExists(path.getParent())) {
                    createDirectories(path.getParent());
                }

                return path;
            }

            ts++;
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
    
    private Path makeRevision(DBBroker broker, Change action, Path logPath, BackupHandler bh, Handler h)
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {

        String id = action.id();
        if (id == null) return null;

        Path folder = null;

        if (action.operation() == Operation.DELETE) {

            lastRevision(action.id(), action.uri(), logPath, h);

        } else {

            XmldbURI url = action.uri();

            if (url == null || url == CommitLog.UNKNOWN_URI) {
                action.uri(uri(id, h));

                if (action.uri() == null || action.uri() == CommitLog.UNKNOWN_URI) return null;
            }

            DocumentImpl doc = broker.getXMLResource(action.uri(), Lock.READ_LOCK);
            if (doc == null) {
                Collection col = broker.getCollection(action.uri());

                if (col == null) {
                    h.error(action.uri(), "not found");
                    return null;
                }

                folder = makeRevision(broker, action.id(), action.uri(), col, logPath, bh, h);

            } else {

                try {
                    folder = makeRevision(broker, action.id(), action.uri(), doc, logPath, bh, h);

                } finally {
                    doc.getUpdateLock().release(Lock.READ_LOCK);
                }
            }
        }
        
        return folder;
    }

    protected XmldbURI uri(String id, Handler h) {
        Metas metas = md.getMetas(id);
        if (metas == null) {
            h.error(id, "missing metas");
            return null;
        }

        String uri = metas.getURI();
        if (uri == null) {
            h.error(id, "missing uri");
            return null;
        }

        try {
            return XmldbURI.create(uri);
        } catch (Exception e) {
            h.error(id, "uri '"+uri+"' is not XmldbURI one");
        }

        return CommitLog.UNKNOWN_URI;
    }


    protected String uuid(XmldbURI uri, Handler h) {
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

    private Path lastRevision(String uuid, XmldbURI uri, Path logPath, Handler h)
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {

        Path revMeta = revFolder(uuid, uuidFolder);

        if (revMeta == null) {
            h.error(uri, "can't create revision file");
            return null;
        }

        try (OutputStream metasStream = Files.newOutputStream(revMeta)) {

            storeLastMetas(logPath.toString(), uuid, uri, metasStream, h);
        }

        h.processed(uri);

        return revMeta;
    }
    
    private Path makeRevision(DBBroker broker, String uuid, XmldbURI uri, Collection col, Path logPath, BackupHandler bh, Handler h)
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {
        
        if (uuid == null) {
            if ((uuid = uuid(uri, h)) == null) return null;
        }

        Path revMeta = revFolder(uuid, uuidFolder);
        
        if (revMeta == null) {
            h.error(uri, "can't create revision file");
            return null;
        }

        processMetas(logPath.toString(), uuid, COL, null, col, revMeta, bh, h);
        
        h.processed(uri);
        
        return revMeta;
    }

    private Path makeRevision(DBBroker broker, String uuid, XmldbURI uri, DocumentImpl doc, Path logPath, BackupHandler bh, Handler h)
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {
        
        if (uuid == null) {
            if ((uuid = uuid(uri, h)) == null) return null;
        }

        Path revPath = revFolder(uuid, uuidFolder);
        
        if (revPath == null) {
            h.error(uri, "can't create revision folder");
            return null;
        }

        String type = XML;

        MessageDigest digest = messageDigest();

        Path dataFile = Files.createTempFile(tmpFolder, "hashing","data");
        
        switch (doc.getResourceType()) {
        case XML_FILE:
            
            //dataFile = folder.resolve(XML_DATA);

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
            
            //dataFile = folder.resolve(BIN_DATA);

            try (InputStream is = broker.getBinaryResource((BinaryDocument)doc)) {
                try (OutputStream fileStream = Files.newOutputStream(dataFile)) {
                
                    DigestOutputStream stream = new DigestOutputStream(fileStream, digest);
                    
                    IOUtils.copy(is, stream);
                }                            
            }

            type = BIN;

            break;
        
        default:
            h.error(uri, "unknown type");
            return null;
        }

        String hash = digestHex(digest);

        //check hash storage
        Path hashPath = resourceFolder(hash, hashesFolder);
        if (Files.notExists(hashPath)) {
            createDirectories(hashPath.getParent());
            Files.move(dataFile, hashPath);
        } else {
            FileUtils.deleteQuietly(dataFile.toFile());
            //if fail it will be clean up next restart or it possible detect old files by last access time
        }

        processMetas(logPath.toString(), uuid, type, hash, doc, revPath, bh, h);
        
        h.processed(uri);
        
        return revPath;
    }
    
    private Path resourceFolder(String docId, Path folder) {
        
        return folder
                .resolve(docId.substring(0, 4))
                .resolve(docId.substring(4, 8))
                .resolve(docId);
    }

    private Path revFolder(String docId, Path folder) throws IOException {
        
        Path location = resourceFolder(docId, folder);

        long nextId = System.currentTimeMillis();
        
        for (int i = 0; i < 1000; i++) {

            Path dir = location.resolve(String.valueOf(nextId));
            if (Files.notExists(dir)) {

                if (Files.notExists(dir.getParent())) {
                    createDirectories(dir.getParent());
                }

                return dir;
            }
            nextId++;
        }
        
        throw new IOException("can't get new id for revision");
    }

    private void processMetas(String logPath, String uuid, String type, String hash, Resource resource, Path location, BackupHandler bh, Handler h) throws XMLStreamException, IOException {

        try (OutputStream metasStream = Files.newOutputStream(location)) {

            processMetas(logPath, uuid, type, hash, resource, metasStream, bh, h);

        }
    }
    
    private String digestHex(MessageDigest digest) {
        return Hex.encodeHexString(digest.digest());
    }
    
    private MessageDigest messageDigest() throws IOException {
        try {
            return MessageDigest.getInstance(MessageDigestAlgorithms.SHA_256);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    private void storeLastMetas(String logPath, String uuid, XmldbURI uri, OutputStream stream, Handler h) throws XMLStreamException, IOException {

        String url = uri.toString();
        String name = uri.lastSegment().toString();

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

        XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();

        XMLStreamWriter writer = outputFactory.createXMLStreamWriter(stream, "UTF-8");

        writer.writeStartDocument("UTF-8", "1.0");
        writer.writeStartElement("RCS", "metas", "http://exist-db.org/RCS");
        writer.writeDefaultNamespace(Namespaces.EXIST_NS);
        writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);

        write(writer, EL_UUID, uuid);
        write(writer, EL_RESOURCE_TYPE, DEL);
        //write(writer, EL_DATA_HASH, DELETED);
        write(writer, EL_LOG_PATH, logPath);

        write(writer, EL_FILE_NAME, name);
        write(writer, EL_FILE_PATH, url);

        if (parentUuid != null) {
            write(writer, PARENT_UUID, parentUuid);
        }

        writer.writeEndElement();
        writer.writeEndDocument();

        writer.flush();
        writer.close();
    }
    
    private void processMetas(String logPath, String uuid, String type, String hash, Resource resource, OutputStream stream, BackupHandler bh, Handler h) throws XMLStreamException, IOException {

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

        final XMLStreamWriter writer = outputFactory.createXMLStreamWriter(stream, "UTF-8");

        writer.writeStartDocument("UTF-8", "1.0");
        writer.writeStartElement("RCS", "metas", "http://exist-db.org/RCS");
        writer.writeDefaultNamespace(Namespaces.EXIST_NS);
        writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);

        write(writer, EL_UUID, uuid);
        write(writer, EL_RESOURCE_TYPE, type);

        if (hash != null) write(writer, EL_DATA_HASH, hash);

        write(writer, EL_LOG_PATH, logPath);

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

        bh.backup(resource, writer);

//        md.streamMetas(uri, new MetaStreamListener() {
//
//            @Override
//            public void metadata(String uuid, String key, Object value) {
//                try {
//                    writer.writeStartElement(MDStorageManager.PREFIX, key, MetaData.NAMESPACE_URI);
//                    writer.writeAttribute(MDStorageManager.PREFIX, MetaData.NAMESPACE_URI, EL_UUID, uuid);
//
//                    if (value instanceof String) {
//                        writer.writeCharacters(value.toString());
//
//                    } else {
//                        //XXX: log?
//                    }
//                    writer.writeEndElement();
//                } catch (XMLStreamException e) {
//                    e.printStackTrace();
//                }
//            }
//
//        });
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

    public InputStream data(String hash) throws IOException {
        if (StringUtils.isEmpty(hash)) throw new IOException("no hash code");

        Path hashPath = resourceFolder(hash, hashesFolder);

        return Files.newInputStream(hashPath);
    }

    public Path data_path(String hash) throws IOException {
        if (StringUtils.isEmpty(hash)) throw new IOException("no hash code");

        return resourceFolder(hash, hashesFolder);
    }

    protected MetasHandler metadata(Path rev) throws IOException {

        try {
            SAXParserFactory parserFactor = SAXParserFactory.newInstance();
            SAXParser parser = parserFactor.newSAXParser();

            MetasHandler dh = new MetasHandler();

            try (InputStream metaStream = Files.newInputStream(rev)) {
                parser.parse(metaStream, dh);
            }

            return dh;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void restore(Path location, Handler h) throws IOException {
        
        DBBroker broker = db.getActiveBroker();

        Path folder = location.resolve("uuids");
        
        try (DirectoryStream<Path> firstDirs = Files.newDirectoryStream(folder)) {
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

        MessageDigest digest = messageDigest();

        OutputStream out = new FakeOutputStream();
        DigestOutputStream digestStream = new DigestOutputStream(out, digest);

        switch (doc.getResourceType()) {
        case XML_FILE:

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

            try (InputStream is = broker.getBinaryResource((BinaryDocument)doc)) {
                IOUtils.copy(is, digestStream);
            }

            break;

        default:
            //h.error(uri, "unknown type");
            return UNKNOWN;
        }
        
        //XXX: make safer
        
        String revHash = readHash(location);
        
        String calcHash = digestHex(digest);
        
        if (revHash.equals(calcHash)) {
            return EQ;
        }
        
        return DIFF;
    }
    
    protected void restoreResource(DBBroker broker, Path location, Handler h) throws IOException {
        Path rev = lastRev(location);
        if (rev != null) {
            try {
                restoreRevision(broker, null, rev, h);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                h.error(location, e);
            }
        }
        //h.error(location, "no revisions");
    }

    protected void restoreRevision(DBBroker broker, XmldbURI newUrl, Path rev, Handler h) throws Exception {

        MetasHandler dh = metadata(rev);

        XmldbURI url = newUrl != null ? newUrl : dh.uri;

        Collection parent = null;
        DocumentImpl doc = null;

        Collection col = broker.getCollection(url);
        if (col == null) {
            parent = broker.getCollection(url.removeLastSegment());

            if (parent != null) {
                doc = parent.getDocument(broker, url.lastSegment());
            }
        }

        try (Txn tx = broker.beginTx()) {

            if (DEL.equals(dh.type)) {

                if (doc != null) {
                    remove(broker, tx, parent, doc);

                } else {
                    broker.removeCollection(tx, col);
                }

            } else if (COL.equals(dh.type)) {

                if (doc != null) remove(broker, tx, parent, doc);

                if (col == null) createCollection(broker, tx, url, dh);

                restoreMetas(broker, col, rev, dh);

            } else {

                if (doc != null) {
                    Lock lock = doc.getUpdateLock();

                    lock.acquire(READ_LOCK);

                    try {
                        switch (checkHash(broker, rev, doc)) {
                            case EQ:
                                restoreMetas(broker, doc, rev, dh);

                                broker.reindexXMLResource(tx, doc);
                                return;
                            case UNKNOWN:
                                restoreMetas(broker, doc, rev, dh);

                                broker.reindexXMLResource(tx, doc);
                                return;
                        }
                    } finally {
                        lock.release(READ_LOCK);
                    }

                    //delete
                    remove(broker, tx, parent, doc); //TODO: KEEP OLD UUID!!!
                }

                createDocument(broker, tx, url, parent, rev, dh);
            }

            tx.success();
        }

        h.processed(url);
    }

    private void createCollection(DBBroker broker, Txn tx, XmldbURI url, MetasHandler dh) throws PermissionDeniedException, IOException, TriggerException {

        RestoreHandler rh = manager.getRestoreHandler();

        Collection collection = broker.getOrCreateCollection(tx, url);

        rh.startRestore(collection, dh.uuid);

//        ResourceMetadata meta = collection.getMetadata();
//        meta.setCreated(dh.createdTime);
//        meta.setLastModified(dh.lastModified);

        broker.saveCollection(tx, collection);

        rh.endRestore(collection);
    }
    
    private void createDocument(DBBroker broker, Txn tx, XmldbURI url, Collection parent, Path rev, MetasHandler dh) throws Exception {

//        listener.setCurrentResource(name);
//        if(currentCollection instanceof Observable) {
//            listener.observe((Observable)currentCollection);
//        }

        RestoreHandler rh = manager.getRestoreHandler();

        DocumentImpl resource;

        if (XML.equals(dh.type)) {
            // store as xml resource

            EXistInputSource is = new FileInputSource( data_path(readHash(rev)).toFile() );

            IndexInfo info = parent.validateXMLResource(tx, broker, url.lastSegment(), is );

            resource = info.getDocument();
            DocumentMetadata meta = resource.getMetadata();
            meta.setMimeType(dh.mimeType);
            meta.setCreated(dh.createdTime);
            meta.setLastModified(dh.lastModified);

//            if((publicid != null) || (systemid != null)) {
//                final DocumentType docType = new DocumentTypeImpl(namedoctype, publicid, systemid);
//                meta.setDocType(docType);
//            }

            rh.startRestore(resource, dh.uuid);

            restoreMetas(broker, resource, rev, dh);

            parent.store(tx, broker, info, is, false);

        } else {
            // store as binary resource

            EXistInputSource is = new FileInputSource( data_path(readHash(rev)).toFile() );

            resource = parent.validateBinaryResource(tx, broker, url.lastSegment(), is.getByteStream(), dh.mimeType, is.getByteStreamLength(), new Date(dh.createdTime), new Date(dh.lastModified));

            rh.startRestore(resource, dh.uuid);

            restoreMetas(broker, resource, rev, dh);

            resource = parent.addBinaryResource(tx, broker, (BinaryDocument)resource, is.getByteStream(), dh.mimeType, is.getByteStreamLength(), new Date(dh.createdTime), new Date(dh.lastModified));
        }

        rh.endRestore(resource);
    }
    
    private void remove(DBBroker broker, Txn tx, Collection docCol, DocumentImpl doc) throws TriggerException, PermissionDeniedException, LockException, TransactionException, IOException {
        if (doc == null) return;

        docCol.removeResource(tx, broker, doc);
    }
    
    private void restoreMetas(DBBroker broker, Resource resource, Path rev, MetasHandler dh) throws Exception {

        SAXParserFactory parserFactor = SAXParserFactory.newInstance();
        SAXParser parser = parserFactor.newSAXParser();

        try (InputStream metaStream = Files.newInputStream(rev)) {
            parser.parse(metaStream, new MetasRestore(broker, resource, dh.uuid));
        }
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

    class MetasRestore extends DefaultHandler {

        DBBroker broker;
        RestoreHandler rh;

        Resource resource;
        String uuid;

        String content = null;

        MetasRestore(DBBroker broker, Resource resource, String uuid) {
            this.broker = broker;
            this.resource = resource;
            this.uuid = uuid;

            //MetaData.PREFIX, EL_METASTORAGE, MetaData.NAMESPACE_URI

            rh = manager.getRestoreHandler();
        }

        public void startDocument () throws SAXException {
            rh.startRestore(resource, uuid);
        }

        public void endDocument() throws SAXException {
            rh.endRestore(resource);
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            content = String.copyValueOf(ch, start, length).trim();

            super.characters(ch, start, length);
        }

        public void startElement (String uri, String localName, String qName, Attributes attributes) throws SAXException {
            rh.startElement(uri, localName, qName, attributes);
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);

            if (content == null) return;

            if (MetaData.NAMESPACE_URI.equals(uri)) {

                rh.endElement(uri, localName, qName);

            } else {

                switch (qName) {
                    case EL_FILE_NAME:
                        //content;
                        break;
                    case EL_FILE_PATH:
                        //XmldbURI.create(content);
                        break;

                    case EL_RESOURCE_TYPE:
                        //content;
                        break;

                    case PARENT_UUID:
                        //content;
                        break;
                    case EL_META_TYPE:
                        //content;
                        break;
                    case EL_CREATED:
                        //DatatypeConverter.parseDateTime(content).getTimeInMillis();
                        break;
                    case EL_LAST_MODIFIED:
                        //DatatypeConverter.parseDateTime(content).getTimeInMillis();
                        break;
                }
            }

            content = null;
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
