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

import org.exist.collections.Collection;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.dom.DocumentMetadata;
import org.exist.dom.QName;
import org.exist.security.Permission;
import org.exist.storage.DBBroker;
import org.exist.storage.MetaStreamListener;
import org.exist.storage.lock.Lock;
import org.exist.storage.md.MetaData;
import org.exist.storage.md.Metas;
import org.exist.storage.serializers.Serializer;
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
            
            if (url.endsWith("/")) {
                throw new XPathException(this, "unimplemented");
            } else {
                
                try {
                    XmldbURI uri = XmldbURI.create(url);
                    
                    DocumentImpl doc = broker.getXMLResource(uri, Lock.READ_LOCK);
                    if (doc == null) {
                        Collection col = broker.getCollection(uri);
                        
                        if (col == null) continue;

                        Path folder = revFolder(md, rcFolder, uri);
                        
                        if (folder == null) continue;

                        processMetas(md, col, null, folder);
                    } else {
                        try {
                            Path folder = revFolder(md, rcFolder, uri);
                            
                            if (folder ==null) continue;
                            
                            Path dataFile;
                            
                            switch (doc.getResourceType()) {
                            case XML_FILE:
                                Files.createDirectories(folder);
                                
                                dataFile = folder.resolve("data.xml");
    
                                Writer writer = Files.newBufferedWriter(dataFile, ENCODING);
    
                                serializer.serialize(doc, writer);
                                
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
                            
                            processMetas(md, null, doc, folder);
    
                        } finally {
                            doc.getUpdateLock().release(Lock.READ_LOCK);
                        }
                    }

                } catch (Exception e) {
                    //XXX: log
                    e.printStackTrace();
                }
            }
            
        }
        
        return Sequence.EMPTY_SEQUENCE;
    }
    
    private Path revFolder(MetaData md, Path rcFolder, XmldbURI uri) throws IOException {
        Metas metas = md.getMetas(uri);
        
        if (metas == null) {
            //XXX: log
            return null;
        }

        String docId = metas.getUUID();
        if (docId == null) {
            //XXX: log
            return null;
        }
        
        Path folder = rcFolder
                .resolve(docId.substring(0, 4))
                .resolve(docId.substring(3, 7))
                .resolve(docId);
        
        long nextId = revNextId(folder);
        
        folder = folder.resolve(String.valueOf(nextId));

        return folder;
    }

    private long revNextId(Path fileFolder) {
        return System.currentTimeMillis();
    }

    private void processMetas(MetaData md, Collection col, DocumentImpl doc, Path folder) throws XMLStreamException, IOException {
        XmldbURI uri;
        String url;
        String name;
        String owner;
        String mimeType;
        long createdTime;
        long lastModified;
        
        if (col != null) {
            uri = col.getURI();
            url = uri.toString();
            name = uri.lastSegment().toString();
            
            mimeType = "collection";
            createdTime = col.getCreationTime();
            lastModified = col.getCreationTime();
            
            Permission perm = col.getPermissionsNoLock();
            
            owner = perm.getOwner().getName();
            
        } else {
            uri = doc.getURI();
            url = uri.toString();
            name = doc.getFileURI().toString();
            
            DocumentMetadata metadata = doc.getMetadata();
            
            mimeType = metadata.getMimeType();
            
            createdTime = metadata.getCreated();
            lastModified = metadata.getLastModified();
            
            owner = doc.getPermissions().getOwner().getName();
        }
        
        GregorianCalendar date = new GregorianCalendar(GMT);

        Path file = folder.resolve("metas");
        
        XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
        final XMLStreamWriter writer = outputFactory.createXMLStreamWriter(Files.newOutputStream(file));
        
        writer.writeStartDocument();
        writer.writeStartElement("RCS", "metas", "http://exist-db.org/RCS");
        writer.writeNamespace("eXist", "http://exist-db.org/");
        writer.writeNamespace(MetaData.PREFIX, MetaData.NAMESPACE_URI);
        
        write(writer, "file-name", name);
        write(writer, "file-path", url);
        write(writer, "owner", owner);
        write(writer, "meta-type", mimeType);
        
        date.setTimeInMillis(createdTime);
        write(writer, "created", DatatypeConverter.printDateTime(date));
        
        date.setTimeInMillis(lastModified);
        write(writer, "created", DatatypeConverter.printDateTime(date));

        writer.writeStartElement(MetaData.PREFIX, "metas", MetaData.NAMESPACE_URI);

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

    private void write(XMLStreamWriter writer, String name, String value) throws XMLStreamException {
        writer.writeStartElement("eXist", name, "http://exist-db.org/");
        writer.writeCharacters(value);
        writer.writeEndElement();
    }
}
