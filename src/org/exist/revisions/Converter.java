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

import org.exist.Database;
import org.exist.Indexer;
import org.exist.collections.Collection;
import org.exist.dom.DocumentImpl;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.util.Configuration;
import org.exist.util.ConfigurationHelper;
import org.exist.xmldb.XmldbURI;
import org.w3c.dom.*;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.util.GregorianCalendar;
import java.util.Iterator;

import static org.exist.Operation.UPDATE;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class Converter {

    Database db;
    RCSManager rcs;

    int count = 0;
    int count_binary = 0;

    Converter(Database db) {
        this.db = db;

        rcs = RCSManager.get();
    }

    public void run() throws Exception {

        try (DBBroker broker = db.authenticate("admin", "")) {

            XmldbURI organizations_uri = XmldbURI.DB.append("organizations");

            Collection organizations = broker.getCollection(organizations_uri);

            Iterator<XmldbURI> it_orgs = organizations.collectionIteratorNoLock(broker);
            while (it_orgs.hasNext()) {

                XmldbURI organization_name = it_orgs.next();

                System.out.println("Organization: " + organization_name);

                XmldbURI organization_uri = organizations_uri.append(organization_name).append("metadata").append("versions");

                Collection resources = broker.getCollection(organization_uri);

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

                        CommitLog log = readMeta(broker, meta, revision_uri);

//                        XmldbURI rev_id = meta.getFileURI();
//
//                        System.out.println("  " + rev_id);
//
//                        if (fcCol == null) {
//                            count_++;
//
//                        } else {
//                            DocumentImpl full_copy = fcCol.getDocument(broker, rev_id);
//
//                            if (full_copy == null) {
//                                count++;
//                            } else if (full_copy.getResourceType() == DocumentImpl.BINARY_FILE) {
//                                count_binary++;
//                            }
//                        }
                    }
                }
            }
        }

        System.out.println(count + " / " + count_binary);
    }

    private CommitLog readMeta(DBBroker broker, DocumentImpl meta, XmldbURI revision_uri) throws Exception {

        CommitLog log = new CommitLog(RCSManager.get(), null);

        NodeList nodes = meta.getDocumentElement().getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Element node = (Element)nodes.item(i);

            System.out.println(node);

            switch (node.getLocalName()) {
                case "properties":
                    readProperties(log, node, revision_uri);

                    break;
                case "additional-version-metadata":
                    readAdditional(log, node);

                    break;
                case "version-data":
                    readVersion(broker, log, node, revision_uri);

                    break;
                default:
                    throw new RuntimeException("unknown node: "+node);
            }
        }

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

    private void readAdditional(CommitLog log, Element element) {

        String properties = log.message();

        NodeList nodes = element.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Element node = (Element)nodes.item(i);

            switch (node.getLocalName()) {
                case "field":
                    //attribute 'name' & text is value

                    String name = node.getAttribute("name");
                    properties += "<"+name+">"+node.getNodeValue()+"</"+name+">";

                    break;
                default:
                    throw new RuntimeException("unknown node: "+node);
            }
        }

        log.message(properties);
    }

    private void readProperties(CommitLog log, Element element, XmldbURI revision_uri) {

        String id = null;
        XmldbURI uri = null;

        String properties = "";

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

                    GregorianCalendar date = new GregorianCalendar(rcs.GMT);

                    date.setTimeInMillis(ts);
                    String str = DatatypeConverter.printDateTime(date);

                    if (rcs.noDots) str = str.replace(':','_');

                    log.id = str;

                    break;
                case "revision": //number
                    properties += "<old-revision-id>"+node.getNodeValue()+"</old-revision-id>";

                    break;
                case "type": //minor or major
                    properties += "<type>"+node.getNodeValue()+"</type>";

                    break;
                case "comment":
                    log.message(node.getNodeValue());

                    break;
                case "changed": //true or false
                    properties += "<changed>"+node.getNodeValue()+"</changed>";

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

        log.message(properties);
    }

    public static void main(String[] args) throws Exception {
        File confFile = ConfigurationHelper.lookup("conf.xml");
        Configuration config = new Configuration(confFile.getAbsolutePath());
        config.setProperty(Indexer.PROPERTY_SUPPRESS_WHITESPACE, "none");
        config.setProperty(Indexer.PRESERVE_WS_MIXED_CONTENT_ATTRIBUTE, Boolean.TRUE);
        BrokerPool.configure(1, 5, config);
        BrokerPool pool = BrokerPool.getInstance();

        Converter converter = new Converter(pool);

        try {
            converter.run();
        } finally {
            pool.shutdown();
        }
    }
}
