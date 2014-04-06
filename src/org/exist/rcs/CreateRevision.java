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

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.exist.Namespaces;
import org.exist.collections.Collection;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.dom.DocumentMetadata;
import org.exist.dom.QName;
import org.exist.security.ACLPermission;
import org.exist.security.Permission;
import org.exist.storage.DBBroker;
import org.exist.storage.MetaStreamListener;
import org.exist.storage.lock.Lock;
import org.exist.storage.md.MetaData;
import org.exist.storage.md.Metas;
import org.exist.storage.serializers.Serializer;
import org.exist.util.serializer.SAXSerializer;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceIterator;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class CreateRevision extends BasicFunction {
    
    final static Charset ENCODING = StandardCharsets.UTF_8;
    
    final static TimeZone GMT = TimeZone.getTimeZone("GMT+0:00");
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("revision-create", Module.NAMESPACE_URI, Module.PREFIX),
            "Create current document state revision.",
            new SequenceType[]{
                new FunctionParameterSequenceType("path", Type.STRING, Cardinality.ONE_OR_MORE,
                "URI paths of documents or collections in database. Collection URIs should end on a '/'.")
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, ""))
    };

    public CreateRevision(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        
        DBBroker broker = context.getBroker();
        
        Serializer serializer = broker.getSerializer();
        serializer.setUser(context.getSubject());
        try {
            serializer.setProperty("omit-xml-declaration", "no");
        } catch (Exception e) {
            throw new XPathException(this, e);
        }

        MetaData md = MetaData.get();
        
        Path dbData = FileSystems.getDefault().getPath(broker.getDataFolder().getAbsolutePath());
        Path rcFolder = dbData.resolve("RCS");
        
        for (SequenceIterator i = args[0].iterate(); i.hasNext();) {
            
            Item item = i.nextItem();
            
            String url = item.getStringValue();
            
//            if (url.endsWith("/")) {
//                throw new XPathException(this, "unimplemented");
//            } else {
                
                try {
                    XmldbURI uri = XmldbURI.create(url);
                    
                    Metas metas = md.getMetas(uri);
                    if (metas == null) {
                        //XXX: log
                        return null;
                    }

                    String uuid = metas.getUUID();
                    if (uuid == null) {
                        //XXX: log
                        return null;
                    }
                    
                    DocumentImpl doc = broker.getXMLResource(uri, Lock.READ_LOCK);
                    if (doc == null) {
                        Collection col = broker.getCollection(uri);
                        
                        if (col == null) continue;

                        Path folder = revFolder(uuid, rcFolder, uri);
                        
                        if (folder == null) continue;

                        Files.createDirectories(folder);

                        processMetas(md, uuid, col, null, folder);
                    } else {
                        try {
                            Path folder = revFolder(uuid, rcFolder, uri);
                            
                            if (folder == null) continue;
                            
                            Path dataFile;
                            
                            switch (doc.getResourceType()) {
                            case XML_FILE:
                                Files.createDirectories(folder);
                                
                                dataFile = folder.resolve("data.xml");
    
                                Writer writer = Files.newBufferedWriter(dataFile, ENCODING);
                                try {
                                    serializer.serialize(doc, writer);
                                } finally {
                                    writer.close();
                                }
                                
                                break;
    
                            case BINARY_FILE:
                                Files.createDirectories(folder);
                                
                                dataFile = folder.resolve("data.bin");
    
                                InputStream is = broker.getBinaryResource((BinaryDocument)doc);
                                try {
                                    Files.copy(is, dataFile);
                                } finally {
                                    is.close();
                                }
    
                                break;
                            
                            default:
                                //log?
                                
                                continue;
                            }
                            
                            processMetas(md, uuid, null, doc, folder);
    
                        } finally {
                            doc.getUpdateLock().release(Lock.READ_LOCK);
                        }
                    }

                } catch (Exception e) {
                    //XXX: log
                    e.printStackTrace();
                }
//            }
            
        }
        
        return Sequence.EMPTY_SEQUENCE;
    }
    
    private Path revFolder(String docId, Path rcFolder, XmldbURI uri) throws IOException {
        
        Path folder = rcFolder
                .resolve(docId.substring(0, 4))
                .resolve(docId.substring(4, 8))
                .resolve(docId);
        
        long nextId = revNextId(folder);
        
        for (int i = 0; i < 5; i++) {
            Path dir = folder.resolve(String.valueOf(nextId));
            if (!Files.exists(dir))
                return dir;
        }
        
        throw new IOException("can't get new id for revision");
    }

    private long revNextId(Path fileFolder) {
        return System.currentTimeMillis();
    }

    private void processMetas(MetaData md, String uuid, Collection col, DocumentImpl doc, Path folder) throws XMLStreamException, IOException {
        XmldbURI uri;
        String url;
        String name;
        String mimeType;

        Permission perm;
        long createdTime;
        long lastModified;
        
        if (col != null) {
            uri = col.getURI();
            url = uri.toString();
            name = uri.lastSegment().toString();
            
            mimeType = "collection";
            createdTime = col.getCreationTime();
            lastModified = col.getCreationTime();
            
            perm = col.getPermissionsNoLock();
            
        } else {
            uri = doc.getURI();
            url = uri.toString();
            name = doc.getFileURI().toString();
            
            DocumentMetadata metadata = doc.getMetadata();
            
            mimeType = metadata.getMimeType();
            
            createdTime = metadata.getCreated();
            lastModified = metadata.getLastModified();
            
            perm = doc.getPermissions();
        }
        
        GregorianCalendar date = new GregorianCalendar(GMT);

        Path file = folder.resolve("metas");
        
        XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        final XMLStreamWriter writer = outputFactory.createXMLStreamWriter(Files.newOutputStream(file));
        
        writer.writeStartDocument();
        writer.writeStartElement("RCS", "metas", "http://exist-db.org/RCS");
        writer.writeDefaultNamespace(Namespaces.EXIST_NS);
        writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);
        
        write(writer, "file-name", name);
        write(writer, "file-path", url);
        write(writer, "meta-type", mimeType);
        
        date.setTimeInMillis(createdTime);
        write(writer, "created", DatatypeConverter.printDateTime(date));
        
        date.setTimeInMillis(lastModified);
        write(writer, "lastModified", DatatypeConverter.printDateTime(date));
        

        writer.writeStartElement("permission");
        writeUnixStylePermissionAttributes(writer, perm);
        if(perm instanceof ACLPermission) {
            writeACLPermission(writer, (ACLPermission)perm);
        }
        writer.writeEndElement();


        writer.writeStartElement(MetaData.PREFIX, "metas", MetaData.NAMESPACE_URI);
        writer.writeAttribute(MetaData.PREFIX, MetaData.NAMESPACE_URI, "uuid", uuid);

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

        writer.writeAttribute("owner", permission.getOwner().getName());
        writer.writeAttribute("group", permission.getGroup().getName());
        writer.writeAttribute("mode", Integer.toOctalString(permission.getMode()));
    }
    
    public static void writeACLPermission(XMLStreamWriter writer, ACLPermission acl) throws XMLStreamException {
        if (acl == null) return;
        
        writer.writeStartElement("acl");
        
        writer.writeAttribute("version", Short.toString(acl.getVersion()));

        for(int i = 0; i < acl.getACECount(); i++) {
            writer.writeStartElement("ace");
            
            writer.writeAttribute("index", Integer.toString(i));
            writer.writeAttribute("target", acl.getACETarget(i).name());
            writer.writeAttribute("who", acl.getACEWho(i));
            writer.writeAttribute("access_type", acl.getACEAccessType(i).name());
            writer.writeAttribute("mode", Integer.toOctalString(acl.getACEMode(i)));

            writer.writeEndElement();
        }
        
        writer.writeEndElement();
    }

    private void write(XMLStreamWriter writer, String name, String value) throws XMLStreamException {
        writer.writeStartElement(name);
        writer.writeCharacters(value);
        writer.writeEndElement();
    }
}
