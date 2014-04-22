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
package org.exist.rcs;

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
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

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
    
    public RCSManager(PluginsManager manager) throws PermissionDeniedException {
        db = manager.getDatabase();

        md = MetaData.get();
        
        Path dbData = FileSystems.getDefault().getPath(db.getActiveBroker().getDataFolder().getAbsolutePath());
        rcFolder = dbData.resolve("RCS");
        
        instance = this;
    }
    
    public void snapshot(Collection collection, Handler h) throws PermissionDeniedException, LockException {

        DBBroker broker = db.getActiveBroker();
        
        Serializer serializer = serializer(broker);
        
        collection.getLock().acquire(Lock.READ_LOCK);
        
        List<Collection> toProcess = new ArrayList<Collection>();
        toProcess.add(collection);
        
        while (!toProcess.isEmpty()) {
            
            List<Collection> nexts = new ArrayList<Collection>();

            for (Collection col : toProcess) {
        
                Iterator<DocumentImpl> docs = col.iteratorNoLock(broker);
                
                while (docs.hasNext()) {
                    DocumentImpl doc = docs.next();
                    
                    try {
                        makeRevision(broker, serializer, doc.getURI(), h);
                    } catch (Exception e) {
                        h.error(doc.getURI(), e);
                    }
                }
                
                List<Collection> next = new ArrayList<Collection>(col.countSubCollection());
    
                Iterator<XmldbURI> cols = col.collectionIteratorNoLock(broker);
                while (cols.hasNext()) {
                    XmldbURI childName = cols.next();
                    
                    XmldbURI uri = col.getURI().append(childName);
                    
                    Collection childColl = broker.getCollection(uri);
                    
                    if (childColl != null) {
                        
                        try {
                            childColl.getLock().acquire(Lock.READ_LOCK);
                            next.add(childColl);
                        
                            makeRevision(broker, serializer, uri, h);
                        } catch (Exception e) {
                            h.error(uri, e);
                        }
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
    }

    public void commit(String msg, Sequence urls, Handler h) throws XPathException {
        
        DBBroker broker = db.getActiveBroker();
        
        Serializer serializer = serializer(broker);

        for (SequenceIterator i = urls.iterate(); i.hasNext();) {
            
            Item item = i.nextItem();
            if (item == null) continue;
            
            String url = item.getStringValue();
            
            XmldbURI uri = XmldbURI.create(url);

            try {
                makeRevision(broker, serializer, uri, h);
            } catch (Exception e) {
                h.error(uri, e);
            }        
        }        
    }
    
    private Serializer serializer(DBBroker broker) {
        Serializer serializer = broker.getSerializer();
        serializer.setUser(broker.getSubject());
        try {
            serializer.setProperty("omit-xml-declaration", "no");
        } catch (Exception e) {}
        
        return serializer;
    }
    
    private void makeRevision(DBBroker broker, Serializer serializer, XmldbURI uri, Handler h) 
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {

        String uuid = null;
        
        Metas metas = md.getMetas(uri);
        if (metas == null) {
            h.error(uri, "missing metas");
            return;
        }

        uuid = metas.getUUID();
        if (uuid == null) {
            h.error(uri, "missing uuid");
            return;
        }
        
        DocumentImpl doc = broker.getXMLResource(uri, Lock.READ_LOCK);
        if (doc == null) {
            Collection col = broker.getCollection(uri);
            
            if (col == null) {
                h.error(uri, "not found");
                return;
            }

            Path folder = revFolder(uuid, rcFolder, uri);
            
            if (folder == null) {
                h.error(uri, "can't create revision folder");
                return;
            }

            Files.createDirectories(folder);

            processMetas(uuid, col, folder, h);
        } else {
            try {
                Path folder = revFolder(uuid, rcFolder, uri);
                
                if (folder == null) {
                    h.error(uri, "can't create revision folder");
                    return;
                }
                Files.createDirectories(folder);

                MessageDigest digest = messageDigest();

                Path dataFile;
                
                switch (doc.getResourceType()) {
                case XML_FILE:
                    
                    dataFile = folder.resolve(XML_DATA);

                    if (digest == null) {
                        try (Writer writer = Files.newBufferedWriter(dataFile, ENCODING)) {
                            serializer.serialize(doc, writer);
                        }
                    } else {
                        try (OutputStream fileStream = Files.newOutputStream(dataFile)) {
                            
                            Writer writerStream = new OutputStreamWriter(
                                new DigestOutputStream(fileStream, digest), 
                                ENCODING.newEncoder()
                            );
                            
                            try (Writer writer = new BufferedWriter(writerStream)) {
                                serializer.serialize(doc, writer);
                            }
                        }
                    }
                    
                    break;

                case BINARY_FILE:
                    
                    dataFile = folder.resolve(BIN_DATA);

                    try (InputStream is = broker.getBinaryResource((BinaryDocument)doc)) {
                        if (digest == null) {
                            Files.copy(is, dataFile);
                        } else {
                            try (OutputStream fileStream = Files.newOutputStream(dataFile)) {
                            
                                DigestOutputStream stream = new DigestOutputStream(fileStream, digest);
                                
                                IOUtils.copy(is, stream);
                            }                            
                        }
                    }

                    break;
                
                default:
                    h.error(uri, "unknown type");
                    return;
                }
                
                writeDigest(dataFile, digest);

                processMetas(uuid, doc, folder, h);

            } finally {
                doc.getUpdateLock().release(Lock.READ_LOCK);
            }
        }
        h.processed(uri); 
    }
    
    private Path revFolder(String docId, Path rcFolder, XmldbURI uri) throws IOException {
        
        Path folder = rcFolder
                .resolve(docId.substring(0, 4))
                .resolve(docId.substring(4, 8))
                .resolve(docId);
        
        long nextId = revNextId(folder);
        
        for (int i = 0; i < 5; i++) {
            Path dir = folder.resolve(String.valueOf(nextId));
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
    
    public static void writeUnixStylePermissionAttributes(XMLStreamWriter writer, Permission permission) throws XMLStreamException {
        if (permission == null) return;

        writer.writeAttribute(AT_OWNER, permission.getOwner().getName());
        writer.writeAttribute(AT_GROUP, permission.getGroup().getName());
        writer.writeAttribute(AT_MODE, Integer.toOctalString(permission.getMode()));
    }
    
    public static void writeACLPermission(XMLStreamWriter writer, ACLPermission acl) throws XMLStreamException {
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

    private void write(XMLStreamWriter writer, String name, String value) throws XMLStreamException {
        writer.writeStartElement(name);
        writer.writeAttribute(name, value);
        writer.writeCharacters(value);
        writer.writeEndElement();
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
            
            DocumentImpl doc = broker.getResource(dh.uri, Permission.WRITE);
            
            if (doc != null) {
                switch (checkHash(broker, rev, doc)) {
                case EQ:
                    return;
                case UNKNOWN:
                    return;
                }
            }
            
            //XXX: code

            if (broker.getCollection(dh.uri) == null)
                System.out.println("restore "+dh.uri);
            
        } catch (Exception e) {
            h.error(location, e);
            return;
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
