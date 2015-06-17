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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import org.exist.Database;
import org.exist.dom.memtree.DocumentBuilderReceiver;
import org.exist.dom.memtree.DocumentImpl;
import org.exist.dom.memtree.MemTreeBuilder;
import org.exist.storage.DBBroker;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.Base64BinaryValueType;
import org.exist.xquery.value.BinaryValue;
import org.exist.xquery.value.BinaryValueFromInputStream;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import static org.exist.revisions.Utils.*;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class Revision implements Comparable<Revision> {
    
    RCSResource resource;
    long id;
    
    Path location;

    //variables for caching
    String type;

    protected Revision(RCSResource resource, Path location) {
        
        id = Long.parseLong( location.getFileName().toString() );
        
        this.resource = resource;
        this.location = location;
    }
    
    public long id() {
        return id;
    }

    @Override
    public int compareTo(Revision o) {
        return Long.compare(id, o.id);
    }

    private String type() throws IOException {
        if (type == null) {
            type = readResourceType(location);
        }
        return type;
    }
    
    public boolean isCollection() throws IOException {
        return Constants.COL.equals(type());
    }

    public boolean isXML() throws IOException {
        return Constants.XML.equals(type());
    }

    public boolean isBinary() throws IOException {
        return Constants.BIN.equals(type());
    }

    public boolean isDeleted() throws IOException {
        return Constants.DEL.equals(type());
    }
    
    public InputStream getData() throws IOException {
        return resource.holder.data(readHash(location));
    }

    public BinaryValue getBinaryValue(XQueryContext context) throws IOException, XPathException {
        return BinaryValueFromInputStream.getInstance(context, new Base64BinaryValueType(), getData());
    }

    public DocumentImpl getXML(Database db) throws IOException, SAXException {
        
        XMLReader reader = null;

        try (InputStream is = getData()) {

            InputSource src = new InputSource(is);

            reader = db.getParserPool().borrowXMLReader();
            
            MemTreeBuilder builder = new MemTreeBuilder();
            builder.startDocument();
            
            DocumentBuilderReceiver receiver = new DocumentBuilderReceiver(builder, true);
            reader.setContentHandler(receiver);
            reader.setProperty("http://xml.org/sax/properties/lexical-handler", receiver);

            reader.parse(src);
            
            builder.endDocument();
            
            return builder.getDocument();

        } finally {
            if (reader != null) {
                db.getParserPool().returnXMLReader(reader);
            }
        }
    }

    public MetasHandler metadata() throws IOException {
        return resource.holder.metadata(location);
    }

    public void restore(DBBroker broker, Handler h) throws Exception {
        resource.holder.restoreRevision(broker, null, location, h);
    }

    public void restore(DBBroker broker, XmldbURI newUrl, Handler h) throws Exception {
        resource.holder.restoreRevision(broker, newUrl, location, h);
    }
}
