/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2015 The eXist Project
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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.exist.*;
import org.exist.EventListener;
import org.exist.backup.BackupHandler;
import org.exist.backup.RestoreHandler;
import org.exist.collections.Collection;
import org.exist.collections.IndexInfo;
import org.exist.collections.triggers.TriggerException;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.dom.DocumentMetadata;
import org.exist.dom.DocumentTypeImpl;
import org.exist.security.ACLPermission;
import org.exist.security.Permission;
import org.exist.security.PermissionDeniedException;
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
import org.w3c.dom.DocumentType;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static java.nio.file.Files.createDirectories;
import static org.exist.dom.DocumentImpl.BINARY_FILE;
import static org.exist.dom.DocumentImpl.XML_FILE;
import static org.exist.revisions.Utils.readHash;
import static org.exist.storage.lock.Lock.READ_LOCK;
import static org.exist.revisions.RCSManager.LOG;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class RCSHolder implements Constants {

    final static int UNKNOWN = -1;
    final static int EQ = 0;
    final static int DIFF = 1;

    RCSManager manager;

    HashStorage hashStorage;

    Path rcFolder;

    Path uuidFolder;
    Path hashesFolder;

    Path commitLogsFolder;
    Path snapshotLogsFolder;

    Path tmpFolder;

    public RCSHolder(RCSManager manager, Path path) throws IOException {
        this.manager = manager;

        if (manager.ver == 2) {
            rcFolder = path;
        } else {
            rcFolder = path.resolve("RCS");
        }

        uuidFolder          = folder("uuids");
        hashesFolder        = folder("hashes");
        commitLogsFolder    = folder("commits");
        snapshotLogsFolder  = folder("snapshots");
        tmpFolder           = folder("tmp");

        //clean up tmp folder
        FileUtils.cleanDirectory(tmpFolder.toFile());

        hashStorage = new HashStorage(hashesFolder, tmpFolder);

    }

    private Path folder(String name) throws IOException {
        return createDirectories(rcFolder.resolve(name));
    }

    public HashStorage hashStorage() {
        return hashStorage;
    }

    public Iterable<CommitReader> commits() {
        return new Commits(this);
    }

    public RCSResource resource(String uuid) {

        Path location = resourceFolder(uuid, uuidFolder);

        if (Files.notExists(location)) return null;

        return new RCSResource(this, location);
    }

    public void snapshot(Collection collection, Handler h) throws PermissionDeniedException, LockException, XMLStreamException, IOException {

        DBBroker broker = manager.db.getActiveBroker();

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
        return manager.addCommit( new CommitLog(this, handler) );
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
                                if (h != null) h.error(uri, e);

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
            if (h != null) h.error(doc.getURI(), e);

            log.writeAttribute("error", e.getMessage());
        }

        log.writeEndElement();

    }

    protected void rollback(CommitLog commitLog) {
        manager.removeCommit(commitLog);
    }

    protected void commit(CommitLog commitLog) throws IOException, XMLStreamException {
        try {
            DBBroker broker = manager.db.getActiveBroker();

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

                if (commitLog.author() != null) {
                    log.writeStartElement("author");
                    log.writeCData(commitLog.author());
                    log.writeEndElement();
                }

                if (commitLog.message() != null) {
                    log.writeStartElement("message");
                    log.writeCData(commitLog.message());
                    log.writeEndElement();
                }

                if (commitLog.metadata != null) {
                    for (Map.Entry<String, String> entry : commitLog.metadata.entrySet()) {
                        log.writeStartElement("metadata");
                        log.writeAttribute("key", entry.getKey());

                        log.writeCData(entry.getValue());

                        log.writeEndElement();
                    }
                }

                for (Change action : commitLog.acts) {

                    //processEntry(doc, broker, serializer, log, logPath, h);

                    log.writeStartElement("entry");

                    if (action.operation() != null) log.writeAttribute("operation", action.operation().name());
                    if (action.id() != null) log.writeAttribute("id", action.id());

                    boolean toWriteURL = true;
                    if (action.isUriSet()) {
                        log.writeAttribute("uri", action.uri().toString());
                        toWriteURL = false;
                    }

                    try {
                        Path folder = makeRevision(broker, action, logRelativePath, bh, commitLog.handler);

                        if (folder != null) {
                            log.writeAttribute("path", rcFolder.relativize(folder).toString());
                            if (toWriteURL) {
                                log.writeAttribute("uri", action.uri().toString());
                            }
                        }

                    } catch (Exception e) {
                        commitLog.handler.error(action.uri(), e);

                        log.writeAttribute("error", e.getMessage());
                    }

                    log.writeEndElement();
                }

                log.writeEndDocument();
            }

            for (EventListener<CommitLog> listener : manager.commitsListener) {
                try {
                    listener.onEvent(commitLog);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        } finally {
            manager.removeCommit(commitLog);
        }
    }

    private synchronized Path logPath(Path location) throws IOException {
        GregorianCalendar date = new GregorianCalendar(GMT);

        long ts = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {

            date.setTimeInMillis(ts);
            String str = DatatypeConverter.printDateTime(date);

            if (manager.noDots) str = str.replace(':','_');

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

    private Path makeRevision(DBBroker broker, Change action, Path logPath, BackupHandler bh, Handler h)
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {

        String id = action.id();
        if (id == null) return null;

        Path folder = null;

        if (action.operation() == Operation.DELETE) {

            lastRevision(action.id(), action.uri(), logPath, h);

        } else {

            if (!action.isUriSet()) {
                action.uri(uri(id, h));

                if (!action.isUriSet()) {
                    return null;
                }
            }

            DocumentImpl doc = broker.getXMLResource(action.uri(), Lock.READ_LOCK);
            if (doc == null) {
                Collection col = broker.getCollection(action.uri());

                if (col == null) {
                    if (h != null) h.error(action.uri(), "not found");
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
        Metas metas = manager.md.getMetas(id);
        if (metas == null) {
            if (h != null) h.error(id, "missing metas");
            return null;
        }

        String uri = metas.getURI();
        if (uri == null) {
            if (h != null) h.error(id, "missing uri");
            return null;
        }

        try {
            return XmldbURI.create(uri);
        } catch (Exception e) {
            if (h != null) h.error(id, "uri '"+uri+"' is not XmldbURI one");
        }

        return Change.UNKNOWN_URI;
    }

    protected String uuid(XmldbURI uri, Handler h) {
        Metas metas = manager.md.getMetas(uri);
        if (metas == null) {
            if (h != null) h.error(uri, "missing metas");
            return null;
        }

        String uuid = metas.getUUID();
        if (uuid == null) {
            if (h != null) h.error(uri, "missing uuid");
            return null;
        }

        return uuid;
    }

    private Path lastRevision(String uuid, XmldbURI uri, Path logPath, Handler h)
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {

        Path revMeta = revFolder(uuid, uuidFolder);

        if (revMeta == null) {
            if (h != null) h.error(uri, "can't create revision file");
            return null;
        }

        try (OutputStream metasStream = Files.newOutputStream(revMeta)) {

            storeLastMetas(logPath.toString(), uuid, uri, metasStream, h);
        }

        if (h != null) h.processed(uri);

        return revMeta;
    }

    private Path makeRevision(DBBroker broker, String uuid, XmldbURI uri, Collection col, Path logPath, BackupHandler bh, Handler h)
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {

        if (uuid == null) {
            if ((uuid = uuid(uri, h)) == null) return null;
        }

        Path revMeta = revFolder(uuid, uuidFolder);

        if (revMeta == null) {
            if (h != null) h.error(uri, "can't create revision file");
            return null;
        }

        processMetas(logPath.toString(), uuid, COL, null, col, revMeta, bh, h);

        if (h != null) h.processed(uri);

        return revMeta;
    }

    protected Path makeRevision(DBBroker broker, String uuid, XmldbURI uri, DocumentImpl doc, Path logPath, BackupHandler bh, Handler h)
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {

        if (uuid == null) {
            if ((uuid = uuid(uri, h)) == null) return null;
        }

        Path revPath = revFolder(uuid, uuidFolder);

        if (revPath == null) {
            if (h != null) h.error(uri, "can't create revision folder");
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
                        manager.serializer(broker).serialize(doc, writer);
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
                if (h != null) h.error(uri, "unknown type");
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

        if (h != null) h.processed(uri);

        return revPath;
    }

    protected Path resourceFolder(String docId, Path folder) {

        return folder
                .resolve(docId.substring(0, 4))
                .resolve(docId.substring(4, 8))
                .resolve(docId);
    }

    protected Path revFolder(String docId, Path folder) throws IOException {

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

            _processMetas(logPath, uuid, type, hash, resource, metasStream, bh, h);

        }
    }

    protected String digestHex(MessageDigest digest) {
        return Hex.encodeHexString(digest.digest());
    }

    protected MessageDigest messageDigest() throws IOException {
        try {
            return MessageDigest.getInstance(MessageDigestAlgorithms.SHA_512);
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
            parentUuid = manager.md.URItoUUID(parentUri);
            if (parentUuid == null) {
                if (h != null) h.error(uri, "missing parent's uuid");
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

    private void _processMetas(String logPath, String uuid, String type, String hash, Resource resource, OutputStream stream, BackupHandler bh, Handler h) throws XMLStreamException, IOException {

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
            parentUuid = manager.md.URItoUUID(parentUri);
            if (parentUuid == null) {
                if (h != null) h.error(uri, "missing parent's uuid");
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

        if (bh != null) bh.backup(resource, writer);

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

    protected <T extends DefaultHandler> T metadata(Path rev, T dh) throws IOException {

        try {
            SAXParserFactory parserFactor = SAXParserFactory.newInstance();
            SAXParser parser = parserFactor.newSAXParser();

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

        DBBroker broker = manager.db.getActiveBroker();

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

                Serializer serializer = manager.serializer(broker);

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
        if (revHash == null) return UNKNOWN;

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
                restoreRevision(broker, null, EMPTY_MAP, rev, h);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                if (h != null) h.error(location, e);
            }
        }
        //if (h != null) h.error(location, "no revisions");
    }

    protected void restoreRevision(DBBroker broker, XmldbURI newUrl, Map<String, String> params, Path rev, Handler h) throws Exception {

        MetasHandler dh = metadata(rev, new MetasHandler());

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
                                break;

                            default:
                                break;
                        }
                    } finally {
                        lock.release(READ_LOCK);
                    }
                }
                uploadDocument(broker, tx, url, parent, rev, params, dh);
            }

            tx.success();
        }

        if (h != null) h.processed(url);
    }

    private void createCollection(DBBroker broker, Txn tx, XmldbURI url, MetasHandler dh) throws PermissionDeniedException, IOException, TriggerException {

        RestoreHandler rh = manager.manager.getRestoreHandler();

        Collection collection = broker.getOrCreateCollection(tx, url);

        rh.startRestore(collection, dh.uuid);

//        ResourceMetadata meta = collection.getMetadata();
//        meta.setCreated(dh.createdTime);
//        meta.setLastModified(dh.lastModified);

        broker.saveCollection(tx, collection);

        rh.endRestore(collection);
    }

    private void uploadDocument(DBBroker broker, Txn tx, XmldbURI url, Collection parent, Path rev, Map<String, String> params, MetasHandler dh) throws Exception {

//        listener.setCurrentResource(name);
//        if(currentCollection instanceof Observable) {
//            listener.observe((Observable)currentCollection);
//        }

        RestoreHandler rh = manager.manager.getRestoreHandler();

        DocumentImpl resource;

        if (XML.equals(dh.type)) {
            // store as xml resource

            XmldbURI name = url.lastSegment();

            DocumentType docType = null;
            if (NO.equals(params.getOrDefault(RESTORE_DOCTYPE, YES))) {
                DocumentImpl doc = parent.getDocument(broker, name);

                docType = doc.getDoctype();
            }

            EXistInputSource is = new FileInputSource( data_path(readHash(rev)).toFile() );

            IndexInfo info = parent.validateXMLResource(tx, broker, name, is );

            resource = info.getDocument();

//            if((publicid != null) || (systemid != null)) {
//                final DocumentType docType = new DocumentTypeImpl(namedoctype, publicid, systemid);
//                meta.setDocType(docType);
//            }

            rh.startRestore(resource, dh.uuid);

            restoreMetas(broker, resource, rev, dh);

            DocumentMetadata meta = resource.getMetadata();
            if (dh.mimeType != null) meta.setMimeType(dh.mimeType);
            if (dh.createdTime != null) meta.setCreated(dh.createdTime);
            if (dh.lastModified != null) meta.setLastModified(dh.lastModified);

            parent.store(tx, broker, info, is, false);

            if (docType != null) {
                docType = new DocumentTypeImpl(docType.getName(), docType.getPublicId(), docType.getSystemId());
                resource.setDocumentType(docType);

                broker.storeXMLResource(tx, resource);
            }

        } else {
            // store as binary resource

            EXistInputSource is = new FileInputSource( data_path(readHash(rev)).toFile() );

            resource = parent.validateBinaryResource(tx, broker, url.lastSegment(), is.getByteStream(), dh.mimeType, is.getByteStreamLength(), new Date(dh.createdTime), new Date(dh.lastModified));

            rh.startRestore(resource, dh.uuid);

            restoreMetas(broker, resource, rev, dh);

//            DocumentMetadata meta = resource.getMetadata();
//            if (dh.mimeType != null) meta.setMimeType(dh.mimeType);
//            if (dh.createdTime != null) meta.setCreated(dh.createdTime);
//            if (dh.lastModified != null) meta.setLastModified(dh.lastModified);

            resource = parent.addBinaryResource(tx, broker, (BinaryDocument)resource, is.getByteStream(), dh.mimeType, is.getByteStreamLength(), new Date(dh.createdTime), new Date(dh.lastModified));

            //workaround because of processing bug at eXist it can't be between validateBinaryResource and addBinaryResource
            DocumentMetadata meta = resource.getMetadata();
            if (dh.mimeType != null) meta.setMimeType(dh.mimeType);
            if (dh.createdTime != null) meta.setCreated(dh.createdTime);
            if (dh.lastModified != null) meta.setLastModified(dh.lastModified);

            broker.storeXMLResource(tx, resource);
            //end of workaround
        }

        rh.endRestore(resource);
    }

    private void remove(DBBroker broker, Txn tx, Collection docCol, DocumentImpl doc) throws TriggerException, PermissionDeniedException, LockException, TransactionException, IOException {
        if (docCol == null || doc == null) return;

        docCol.removeResource(tx, broker, doc);
    }

    private void restoreMetas(DBBroker broker, Resource resource, Path rev, MetasHandler dh) throws Exception {

        SAXParserFactory parserFactor = SAXParserFactory.newInstance();
        SAXParser parser = parserFactor.newSAXParser();

        try (InputStream metaStream = Files.newInputStream(rev)) {
            parser.parse(metaStream, new MetasRestore(broker, resource, dh));
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
        MetasHandler dh;
        RestoreHandler rh;

        Resource resource;
        String uuid;

        String content = null;

        MetasRestore(DBBroker broker, Resource resource, MetasHandler dh) {
            this.broker = broker;
            this.resource = resource;
            this.dh = dh;
            uuid = dh.uuid;

            //MetaData.PREFIX, EL_METASTORAGE, MetaData.NAMESPACE_URI

            rh = manager.manager.getRestoreHandler();
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

            if (MetaData.NAMESPACE_URI.equals(uri)) {

                rh.endElement(uri, localName, qName);

            } else {
                if (content == null) return;

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
                        dh.mimeType = content;
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
