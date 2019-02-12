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

import org.exist.Namespaces;
import org.exist.backup.BackupDescriptor;
import org.exist.util.EXistInputSource;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.util.Stack;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class BackupHandler extends DefaultHandler {

    public final static String MD_NAMESPACE_URI = "http://exist-db.org/metadata";
    public final static String MD_UUID = "uuid";

    private final static SAXParserFactory saxFactory = SAXParserFactory.newInstance();
    static {
        saxFactory.setNamespaceAware(true);
        saxFactory.setValidating(false);
    }

    private final ImportAnalyzer master;
    private final BackupDescriptor descriptor;

    XmldbURI curCol;
    Stack<XmldbURI> urls = new Stack<>();
    Stack<String> uuids = new Stack<>();

    public BackupHandler(ImportAnalyzer master, BackupDescriptor descriptor) {
        this.master = master;
        this.descriptor = descriptor;
    }

    @Override
    public void startDocument() throws SAXException {
        master.listener.setCurrentBackup(descriptor.getSymbolicPath());
    }
    
    /**
     * @see  org.xml.sax.ContentHandler#startElement(String, String, String, Attributes)
     */
    @Override
    public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {

        if (namespaceURI == null || !namespaceURI.equals(Namespaces.EXIST_NS)) {
            return;
        }

        String parentUuid = null;
        if (!uuids.isEmpty()) {
            parentUuid = uuids.peek();
        }

        if ("collection".equals(localName)) {

            String uuid = entryCollection(atts);
            uuids.push(uuid);

            if (parentUuid != null) {
                master.parentOfUuid.put(uuid, parentUuid);
            }

        } else if ("resource".equals(localName)) {

            String uuid = entryDocument(atts);

            if (parentUuid != null) {
                master.parentOfUuid.put(uuid, parentUuid);
            }

        } else if ("subcollection".equals(localName)) {
            restoreSubCollectionEntry(atts);
//        } else if ("deleted".equals(localName)) {
//            restoreDeletedEntry(atts);
//        } else if ("ace".equals(localName)) {
//            addACEToDeferredPermissions(atts);
        }
    }
    
    @Override
    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {

        if (!namespaceURI.equals(Namespaces.EXIST_NS)) {
            return;
        }

        if ("collection".equals(localName)) {
            uuids.pop();

            urls.pop();

            if (urls.isEmpty()) {
                curCol = null;
            } else {
                curCol = urls.peek();
            }
        }

//        rh.endElement(namespaceURI, localName, qName);

        super.endElement(namespaceURI, localName, qName);
    }
    
    private String entryCollection(Attributes atts) throws SAXException {

        final String skip = atts.getValue( "skip" );

        //dont process entries which should be skipped
        if(skip != null && !"no".equals(skip)) {
            return null;
        }

        String name = atts.getValue("name");
        if (name == null) {
            throw new SAXException("Collection requires a name attribute");
        }

        urls.push(curCol);
        curCol = XmldbURI.create(name);

        String uuid = atts.getValue(MD_NAMESPACE_URI, MD_UUID);
        if (uuid == null) {
            master.listener.error("no uuid '"+name+"'");
        } else {

            String url = master.uuidToUrl.get(uuid);

            if (url != null && !url.equals(name)) {
                master.listener.error("duplicate '"+uuid+"': '"+url+"' vs '"+name+"'");
                master.stopError = true;
                return null;
            }

            master.uuidToUrl.put(uuid, name);
            master.colToUuid.put(name, uuid);

            return uuid;
        }
        return null;
    }

    private String entryDocument(Attributes atts) throws SAXException {

        final String skip = atts.getValue( "skip" );

        //dont process entries which should be skipped
        if(skip != null && !"no".equals(skip)) {
            return null;
        }

        String name = atts.getValue("name");
        if (name == null) {
            throw new SAXException("Collection requires a name attribute");
        }

        name = curCol.append(name).toString();

        String uuid = atts.getValue(MD_NAMESPACE_URI, MD_UUID);
        if (uuid == null) {
            master.listener.error("no uuid '"+name+"'");
        } else {

            String url = master.uuidToUrl.get(uuid);

            if (url != null && !url.equals(name)) {
                master.listener.error("duplicate '"+uuid+"': '"+url+"' vs '"+name+"'");
                master.stopError = true;
                return null;
            }

            master.uuidToUrl.put(uuid, name);
            master.docToUuid.put(name, uuid);

            return uuid;
        }
        return null;
    }

    private void restoreSubCollectionEntry(Attributes atts) throws SAXException {

        final String name;
        if(atts.getValue("filename") != null) {
            name = atts.getValue("filename");
        } else {
            name = atts.getValue("name");
        }

        //parse the sub-collection descriptor and restore
        final BackupDescriptor subDescriptor = descriptor.getChildBackupDescriptor(name);
        if(subDescriptor != null) {

            final SAXParser sax;
            try {
                sax = saxFactory.newSAXParser();

                final XMLReader reader = sax.getXMLReader();

                final EXistInputSource is = subDescriptor.getInputSource();
                is.setEncoding( "UTF-8" );

                reader.setContentHandler(master.handler(subDescriptor));
                reader.parse(is);
            } catch(final SAXParseException e) {
                throw new SAXException("Could not process collection: " + descriptor.getSymbolicPath(name, false), e);
            } catch(final ParserConfigurationException pce) {
                throw new SAXException("Could not initalise SAXParser for processing sub-collection: " + descriptor.getSymbolicPath(name, false), pce);
            } catch(final IOException ioe) {
                throw new SAXException("Could not read sub-collection for processing: " + ioe.getMessage(), ioe);
            }
        } else {
            master.listener.error("Collection " + descriptor.getSymbolicPath(name, false) + " does not exist or is not readable.");
        }
    }


//    private void restoreDeletedEntry(Attributes atts) {
//        final String name = atts.getValue("name");
//        final String type = atts.getValue("type");
//
//        if ("collection".equals(type)) {
//
//            //TODO: currentCollection + name;
//
//        } else if("resource".equals(type)) {
//            //TODO:
//        }
//    }
}