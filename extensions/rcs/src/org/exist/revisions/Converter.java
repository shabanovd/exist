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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.exist.*;
import org.exist.collections.Collection;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.md.MetaData;
import org.exist.util.Configuration;
import org.exist.util.ConfigurationHelper;
import org.exist.xmldb.XmldbURI;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.bind.DatatypeConverter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

import static java.nio.file.Files.createDirectories;
import static org.exist.Operation.UPDATE;
import static org.exist.dom.DocumentImpl.*;
import static org.exist.revisions.RCSManager.LOG;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class Converter implements Constants {

    JsonFactory f = new JsonFactory();

    Database db;

    int count = 0;
    int count_ = 0;
    int count_binary = 0;

    public Converter(Database db) {
        this.db = db;
    }

    public void run() throws Exception {

        RCSManager rcs = RCSManager.get();

        DBBroker broker = db.getActiveBroker();

        XmldbURI organizations_uri = XmldbURI.DB.append("organizations");

        Collection organizations = broker.getCollection(organizations_uri);

        Iterator<XmldbURI> it_orgs = organizations.collectionIteratorNoLock(broker);
        while (it_orgs.hasNext()) {

            XmldbURI organization_name = it_orgs.next();

            System.out.println("Organization: " + organization_name);

            RCSHolder holder = rcs.getOrCreateHolder(organization_name.lastSegment().toString());

            XmldbURI organization_uri = organizations_uri.append(organization_name).append("metadata").append("versions");

            Collection resources = broker.getCollection(organization_uri);
            if (resources == null) {
                System.out.println("SKIP: "+organization_uri);
                continue;
            }

            Iterator<XmldbURI> it_rs = resources.collectionIteratorNoLock(broker);
            while (it_rs.hasNext()) {

                XmldbURI resource_name = it_rs.next();

                System.out.println(" "+resource_name);

                XmldbURI revision_uri = organization_uri.append(resource_name);
                Collection revisions = broker.getCollection(revision_uri);

                Collection.CollectionEntry fc_url = revisions.getSubCollectionEntry(broker, "exist-versions");
                Collection fcCol = broker.getCollection(fc_url.getUri());

                Iterator<DocumentImpl> it_revs = revisions.iterator(broker);
                while (it_revs.hasNext()) {

                    DocumentImpl meta = it_revs.next();

                    CommitLog commitLog = readMeta(holder, broker, meta, revision_uri);

                    String str = commitLog.id;
                    Path logPath = holder.commitLogsFolder.resolve( str.substring(0, 7) ).resolve( str );

                    Files.createDirectories(logPath.getParent());

                    writeLog(holder, broker, fcCol, meta, logPath, commitLog);

                    for (EventListener<CommitLog> listener : RCSManager.get().commitsListener) {
                        try {
                            listener.onEvent(commitLog);
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                        }
                    }
                }
            }
        }

        System.out.println(count + " / " + count_ + " / " + count_binary);
    }

    private Path write_data(RCSHolder holder, DBBroker broker, Collection fcCol, DocumentImpl meta, Change action, Path logPath) throws PermissionDeniedException, SAXException, IOException, XMLStreamException {
        XmldbURI rev_id = meta.getFileURI();

        System.out.println("  " + rev_id);

        if (fcCol == null) {
            count_++;

        } else {
            DocumentImpl doc = fcCol.getDocument(broker, rev_id);

            if (doc == null) {
                count++;
            } else {
                if (doc.getResourceType() == DocumentImpl.BINARY_FILE) {
                    count_binary++;
                }

                return makeRevision(holder, broker, action.id(), action.uri(), doc, logPath);
            }
        }

        return null;
    }

    private void writeLog(RCSHolder holder, DBBroker broker, Collection fcCol, DocumentImpl meta, Path logPath, CommitLog commitLog) throws IOException, XMLStreamException {

        Path logRelativePath = holder.rcFolder.relativize(logPath);

        try (BufferedWriter commitLogStream = Files.newBufferedWriter(logPath, ENCODING)) {
            XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();

            XMLStreamWriter log = outputFactory.createXMLStreamWriter(commitLogStream);

            log.writeStartDocument();

            log.writeStartElement("RCS", "commit-log", "http://exist-db.org/RCS");
            log.writeDefaultNamespace(Namespaces.EXIST_NS);
            //writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);

            log.writeAttribute("id", commitLog.id());

            log.writeStartElement("author");
            log.writeCData(commitLog.author());
            log.writeEndElement();

            log.writeStartElement("message");
            log.writeCData(commitLog.message());
            log.writeEndElement();

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

                try {
                    Path folder = write_data(holder, broker, fcCol, meta, action, logRelativePath);

                    if (folder != null) {
                        log.writeAttribute("uri", action.uri().toString());
                        log.writeAttribute("path", holder.rcFolder.relativize(folder).toString());
                    }

                } catch (Exception e) {
                    log.writeAttribute("error", e.getMessage());
                }

                log.writeEndElement();
            }

            log.writeEndDocument();
        }
    }

    private CommitLog readMeta(RCSHolder holder, DBBroker broker, DocumentImpl meta, XmldbURI revision_uri) throws Exception {

        StringWriter writer = new StringWriter();

        JsonGenerator json = f.createJsonGenerator(writer);
        json.writeStartObject();

        CommitLog log = new CommitLog(holder, null);

        NodeList nodes = meta.getDocumentElement().getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Element node = (Element)nodes.item(i);

//            System.out.println(node);

            switch (node.getLocalName()) {
                case "properties":
                    readProperties(json, log, node, revision_uri);

                    break;
                case "additional-version-metadata":
                    readAdditional(json, log, node);

                    break;
                case "version-data":
                    readVersion(broker, log, node, revision_uri);

                    break;
                default:
                    throw new RuntimeException("unknown node: "+node);
            }
        }
        json.writeEndObject();
        json.close();

        log.message(writer.getBuffer().toString());

        return log;
    }

    private void readVersion(DBBroker broker, CommitLog log, Element element, XmldbURI revision_uri) throws Exception {

        //attribute 'uri' point to ?

        NodeList nodes = element.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            if (!(nodes.item(i) instanceof Attr)) throw new RuntimeException("'version-data' element have children");
        }

        String uri = element.getAttribute("uri");

        System.out.println(uri);

        XmldbURI resource_uri = revision_uri.append(XmldbURI.create(uri));

        DocumentImpl doc = (DocumentImpl) broker.getXMLResource(resource_uri);

        if (doc == null) {
            count++;
            return;
        }

        if (doc.getResourceType() == DocumentImpl.BINARY_FILE) count_binary++;
    }

    private void readAdditional(JsonGenerator json, CommitLog log, Element element) throws IOException {

        NodeList nodes = element.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Element node = (Element)nodes.item(i);

            switch (node.getLocalName()) {
                case "field":
                    //attribute 'name' & text is value

                    String name = node.getAttribute("name");
                    json.writeObjectField(name, node.getNodeValue());

                    break;
                default:
                    throw new RuntimeException("unknown node: "+node);
            }
        }
    }

    private void readProperties(JsonGenerator json, CommitLog log, Element element, XmldbURI revision_uri) throws IOException {

        String id = null;
        XmldbURI uri = null;

        NodeList nodes = element.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);

            if (!(n instanceof Element)) continue;

            Element node = (Element) n;

            switch (node.getLocalName()) {
                case "document-uuid":
                    id = node.getNodeValue();

                    break;
                case "document": //full url
                    uri = XmldbURI.create(node.getNodeValue());

                    break;
                case "user":
                    log.author(node.getNodeValue());

                    break;
                case "date":
                    //2013-05-28T17:23:12.094Z
                    long ts = DatatypeConverter.parseDateTime(node.getNodeValue()).getTimeInMillis();

                    GregorianCalendar date = new GregorianCalendar(GMT);

                    date.setTimeInMillis(ts);
                    String str = DatatypeConverter.printDateTime(date);

                    if (RCSManager.get().noDots) str = str.replace(':','_');

                    log.id = str;

                    break;
                case "revision": //number
                    json.writeObjectField("old-revision-id", node.getNodeValue());

                    break;
                case "type": //minor or major
                    json.writeObjectField("type", node.getNodeValue());

                    break;
                case "comment":
                    json.writeObjectField("comment", node.getNodeValue());

                    break;
                case "changed": //true or false
                    json.writeObjectField("changed", node.getNodeValue());

                    break;
                default:
                    throw new RuntimeException("unknown node: "+node);
            }
        }

        String res_id = revision_uri.lastSegment().toString();

        if (id != null) {
            if (!id.equals(res_id)) throw new RuntimeException("id different");
        } else id = res_id;

        if (uri == null) throw new RuntimeException("no uri");

        log.delete(id, uri);
        ((CommitLog.Action) log.acts.get(0)).op = UPDATE;
    }

    protected Path makeRevision(RCSHolder holder, DBBroker broker, String uuid, XmldbURI uri, DocumentImpl doc, Path logPath)
            throws IOException, PermissionDeniedException, SAXException, XMLStreamException {

        if (uuid == null) throw new RuntimeException("no uuid");

        Path revPath = holder.revFolder(uuid, holder.uuidFolder);

        if (revPath == null) throw new RuntimeException("can't create revision folder");

        String type = XML;

        MessageDigest digest = holder.messageDigest();

        Path dataFile = Files.createTempFile(holder.tmpFolder, "hashing", "data");

        switch (doc.getResourceType()) {
            case XML_FILE:

                //dataFile = folder.resolve(XML_DATA);

                try (OutputStream fileStream = Files.newOutputStream(dataFile)) {

                    Writer writerStream = new OutputStreamWriter(
                            new DigestOutputStream(fileStream, digest),
                            ENCODING.newEncoder()
                    );

                    try (Writer writer = new BufferedWriter(writerStream)) {
                        RCSManager.get().serializer(broker).serialize(doc, writer);
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
                throw new RuntimeException("unknown type");
        }

        String hash = holder.digestHex(digest);

        //check hash storage
        Path hashPath = holder.resourceFolder(hash, holder.hashesFolder);
        if (Files.notExists(hashPath)) {
            createDirectories(hashPath.getParent());
            Files.move(dataFile, hashPath);
        } else {
            FileUtils.deleteQuietly(dataFile.toFile());
            //if fail it will be clean up next restart or it possible detect old files by last access time
        }

        processMetas(logPath.toString(), uuid, type, hash, uri, doc, revPath);

        return revPath;
    }

    private void processMetas(String logPath, String uuid, String type, String hash, XmldbURI uri, Resource resource, Path location) throws XMLStreamException, IOException {
        try (OutputStream stream = Files.newOutputStream(location)) {
            _processMetas(logPath, uuid, type, hash, uri, resource, stream);
        }
    }

    private void _processMetas(String logPath, String uuid, String type, String hash, XmldbURI uri, Resource resource, OutputStream stream) throws XMLStreamException, IOException {

//        XmldbURI uri = resource.getURI();
        String url = uri.toString();
        String name = uri.lastSegment().toString();

        ResourceMetadata metadata = resource.getMetadata();

        String mimeType = metadata.getMimeType();

//        long createdTime = metadata.getCreated();
//        long lastModified = metadata.getLastModified();
//
//        Permission perm = resource.getPermissions();

//        String parentUuid = null;
//        XmldbURI parentUri = uri.removeLastSegment();
//
//        if (!(uri.equalsInternal(XmldbURI.DB)
//                || parentUri.equalsInternal(XmldbURI.DB)
//                || parentUri.equalsInternal(XmldbURI.EMPTY_URI)
//        )
//                ) {
//            parentUuid = md.URItoUUID(parentUri);
//            if (parentUuid == null) {
//                if (h != null) h.error(uri, "missing parent's uuid");
//            }
//        }

//        GregorianCalendar date = new GregorianCalendar(GMT);

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

//        if (parentUuid != null) {
//            write(writer, PARENT_UUID, parentUuid);
//        }

        write(writer, EL_META_TYPE, mimeType);

//        date.setTimeInMillis(createdTime);
//        write(writer, EL_CREATED, DatatypeConverter.printDateTime(date));
//
//        date.setTimeInMillis(lastModified);
//        write(writer, EL_LAST_MODIFIED, DatatypeConverter.printDateTime(date));


//        writer.writeStartElement(EL_PERMISSION);
//        writeUnixStylePermissionAttributes(writer, perm);
//        if(perm instanceof ACLPermission) {
//            writeACLPermission(writer, (ACLPermission)perm);
//        }
//        writer.writeEndElement();


//        writer.writeStartElement(MetaData.PREFIX, EL_METASTORAGE, MetaData.NAMESPACE_URI);
//        writer.writeAttribute(MetaData.PREFIX, MetaData.NAMESPACE_URI, EL_UUID, uuid);
//
//        if (bh != null) bh.backup(resource, writer);
//
//        writer.writeEndElement();

        writer.writeEndElement();
        writer.writeEndDocument();

        writer.flush();
        writer.close();
    }

    private void write(XMLStreamWriter w, String name, String value) throws XMLStreamException {
        w.writeStartElement(name);
        //writer.writeAttribute(name, value);
        w.writeCharacters(value);
        w.writeEndElement();
    }

    public static void main(String[] args) throws Exception {
        File confFile = ConfigurationHelper.lookup("conf.xml");
        Configuration config = new Configuration(confFile.getAbsolutePath());
        config.setProperty(Indexer.PROPERTY_SUPPRESS_WHITESPACE, "none");
        config.setProperty(Indexer.PRESERVE_WS_MIXED_CONTENT_ATTRIBUTE, Boolean.TRUE);
        BrokerPool.configure(1, 5, config);
        BrokerPool pool = BrokerPool.getInstance();

        Converter converter = new Converter(pool);

        System.out.println("password: ");
        Scanner s = new Scanner(System.in);
        String pass = s.next();

        try (DBBroker broker = pool.authenticate("admin", pass)) {
            converter.run();
        } finally {
            pool.shutdown();
        }
    }
}
