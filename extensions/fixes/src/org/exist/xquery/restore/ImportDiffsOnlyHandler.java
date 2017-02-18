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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.Namespaces;
import org.exist.backup.*;
import org.exist.backup.restore.listener.RestoreListener;
import org.exist.collections.Collection;
import org.exist.collections.IndexInfo;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.dom.DocumentMetadata;
import org.exist.dom.DocumentTypeImpl;
import org.exist.security.ACLPermission;
import org.exist.security.ACLPermission.ACE_ACCESS_TYPE;
import org.exist.security.ACLPermission.ACE_TARGET;
import org.exist.security.Permission;
import org.exist.security.PermissionDeniedException;
import org.exist.security.SecurityManager;
import org.exist.security.internal.aider.ACEAider;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock.LockMode;
import org.exist.storage.md.*;
import org.exist.storage.serializers.ChainOfReceiversFactory;
import org.exist.storage.serializers.EXistOutputKeys;
import org.exist.storage.serializers.Serializer;
import org.exist.storage.txn.TransactionManager;
import org.exist.storage.txn.Txn;
import org.exist.util.EXistInputSource;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.XPathException;
import org.exist.xquery.restore.deferred.*;
import org.exist.xquery.util.URIUtils;
import org.exist.xquery.value.DateTimeValue;
import org.w3c.dom.DocumentType;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static org.exist.backup.SystemExport.CONFIG_FILTERS;
import static org.exist.dom.DocumentImpl.BINARY_FILE;
import static org.exist.dom.DocumentImpl.XML_FILE;

public class ImportDiffsOnlyHandler extends DefaultHandler {

    private final static Logger LOG = LogManager.getLogger(ImportDiffsOnlyHandler.class);
    private final static SAXParserFactory saxFactory = SAXParserFactory.newInstance();
    static {
        saxFactory.setNamespaceAware(true);
        saxFactory.setValidating(false);
    }
    private static final int STRICT_URI_VERSION = 1;

    final static int UNKNOWN = -1;
    final static int EQ = 0;
    final static int DIFF = 1;

    Charset ENCODING = StandardCharsets.UTF_8;

    private ChainOfReceiversFactory chainFactory;

    private DBBroker broker;

    private org.exist.backup.RestoreHandler rh;

    private final RestoreListener listener;
    private final String dbBaseUri;
    private final BackupDescriptor descriptor;

    private final ImportAnalyzer master;

    //handler state
    private int version = 0;
    private Collection currentCollection;
    private Stack<DeferredPermission> deferredPermissions = new Stack<>();

    MetaData md;
    RestoreMetadata rmd;

    public ImportDiffsOnlyHandler(DBBroker broker, RestoreListener listener, String dbBaseUri, BackupDescriptor descriptor, ImportAnalyzer master) {
        this.broker = broker;
        this.listener = listener;
        this.dbBaseUri = dbBaseUri;
        this.descriptor = descriptor;
        this.master = master;

        rh = broker.getDatabase().getPluginsManager().getRestoreHandler();

        md = MetaData.get();
        rmd = new RestoreMetadata(md);

        List<String> list = (List<String>) broker.getConfiguration().getProperty(CONFIG_FILTERS);
        if (list != null) {
            chainFactory = new ChainOfReceiversFactory(list);
        }
    }

    @Override
    public void startDocument() throws SAXException {
        listener.setCurrentBackup(descriptor.getSymbolicPath());
        rh.startDocument();
    }

    /**
     * @see  org.xml.sax.ContentHandler#startElement(String, String, String, Attributes)
     */
    @Override
    public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {

        //only process entries in the exist namespace
//        if(namespaceURI != null && !namespaceURI.equals(Namespaces.EXIST_NS)) {
//            return;
//        }

        DeferredPermission df = null;

        if (namespaceURI != null && namespaceURI.equals(Namespaces.EXIST_NS)) {
            if ("collection".equals(localName) || "resource".equals(localName)) {

                if ("collection".equals(localName)) {
                    df = restoreCollectionEntry(atts);

                    rmd.colPairs = new ArrayList<>();
                    rmd.colURL = df.url();
                } else {
                    df = restoreResourceEntry(atts);

                    rmd.docPairs = new ArrayList<>();
                    rmd.docURL = df.url();
                }

                deferredPermissions.push(df);

            } else if ("subcollection".equals(localName)) {
                restoreSubCollectionEntry(atts);
            } else if ("deleted".equals(localName)) {
                restoreDeletedEntry(atts);
            } else if ("ace".equals(localName)) {
                addACEToDeferredPermissions(atts);
            }
        }
        //rh.startElement(namespaceURI, localName, qName, atts);

        //collect metas
        String uuid = rmd.startElement(namespaceURI, localName, qName, atts);
        if (uuid != null && df != null) {
            df.uuid(uuid);
        }
    }

    @Override
    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {

        if(namespaceURI != null && namespaceURI.equals(Namespaces.EXIST_NS)
                && ("collection".equals(localName) || "resource".equals(localName))) {

            setDeferredPermissions();
        }

        rmd.endElement(namespaceURI, localName, qName);

        //rh.endElement(namespaceURI, localName, qName);


        super.endElement(namespaceURI, localName, qName);
    }

    private DeferredPermission restoreCollectionEntry(Attributes atts) throws SAXException {

        final String name = atts.getValue("name");

        if(name == null) {
            throw new SAXException("Collection requires a name attribute");
        }

        final String owner = atts.getValue("owner");
        final String group = atts.getValue("group");
        final String mode = atts.getValue("mode");
        final String created = atts.getValue("created");
        final String strVersion = atts.getValue("version");

        if(strVersion != null) {
            try {
                this.version = Integer.parseInt(strVersion);
            } catch(final NumberFormatException nfe) {
                final String msg = "Could not parse version number for Collection '" + name + "', defaulting to version 0";
                listener.warn(msg);
                LOG.warn(msg);

                this.version = 0;
            }
        }

        Date date_created = null;

        if(created != null) {
            try {
                date_created = (new DateTimeValue(created)).getDate();
            } catch(final XPathException xpe) {
                listener.warn("Illegal creation date. Ignoring date...");
            }
        }

        try {
            listener.createCollection(name);
            XmldbURI collUri;

            if(version >= STRICT_URI_VERSION) {
                collUri = XmldbURI.create(name);
            } else {
                try {
                    collUri = URIUtils.encodeXmldbUriFor(name);
                } catch(final URISyntaxException e) {
                    listener.warn("Could not parse document name into a URI: " + e.getMessage());
                    return new SkippedEntryDeferredPermission();
                }
            }

            final TransactionManager txnManager = broker.getDatabase().getTransactionManager();
            try (Txn txn = txnManager.beginTransaction()) {
                currentCollection = broker.getOrCreateCollection(txn, collUri);

                currentCollection.setCreationTime(date_created.getTime());

                rh.startRestore(currentCollection, atts);

                broker.saveCollection(txn, currentCollection);

                txn.success();
            } catch (final Exception e) {
                throw new SAXException(e);
            }

            listener.setCurrentCollection(name);

            if(currentCollection == null) {
                throw new SAXException("Collection not found: " + collUri);
            }

            Integer m = Permission.DEFAULT_COLLECTION_PERM;
            try {
                m = Integer.parseInt(mode, 8);
            } catch (NumberFormatException e) {
                LOG.warn("Collection '"+collUri+"' mode is invalid '"+mode+"', use default!");
            }

            final CollectionDeferredPermission deferred;
            if(name.startsWith(XmldbURI.SYSTEM_COLLECTION)) {
                //prevents restore of a backup from changing System collection ownership
                deferred = new CollectionDeferredPermission(listener, currentCollection, SecurityManager.SYSTEM, SecurityManager.DBA_GROUP, m);
            } else {
                deferred = new CollectionDeferredPermission(listener, currentCollection, owner, group, m);
            }

            deferred.url = collUri;

            //called by rh.endElement
            //rh.endRestore(currentCollection);

            return deferred;

        } catch(final Exception e) {
            final String msg = "An unrecoverable error occurred while restoring\ncollection '" + name + "'. " + "Aborting restore!";
            LOG.error(msg, e);
            listener.warn(msg);
            throw new SAXException(e.getMessage(), e);
        }
    }

    private void restoreSubCollectionEntry(Attributes atts) throws SAXException {

        final String name;
        if(atts.getValue("filename") != null) {
            name = atts.getValue("filename");
        } else {
            name = atts.getValue("name");
        }

        //exclude /db/system collection and sub-collections, as these have already been restored
//        if ((currentCollection.getURI().startsWith(XmldbURI.SYSTEM)))
//            return;

        //parse the sub-collection descriptor and restore
        final BackupDescriptor subDescriptor = descriptor.getChildBackupDescriptor(name);
        if(subDescriptor != null) {

            final SAXParser sax;
            try {
                sax = saxFactory.newSAXParser();

                final XMLReader reader = sax.getXMLReader();

                final EXistInputSource is = subDescriptor.getInputSource();
                is.setEncoding( "UTF-8" );

                final ImportDiffsOnlyHandler handler = new ImportDiffsOnlyHandler(broker, listener, dbBaseUri, subDescriptor, master);

                reader.setContentHandler(handler);
                reader.parse(is);
            } catch(final SAXParseException e) {
                throw new SAXException("Could not process collection: " + descriptor.getSymbolicPath(name, false), e);
            } catch(final ParserConfigurationException pce) {
                throw new SAXException("Could not initalise SAXParser for processing sub-collection: " + descriptor.getSymbolicPath(name, false), pce);
            } catch(final IOException ioe) {
                throw new SAXException("Could not read sub-collection for processing: " + ioe.getMessage(), ioe);
            }
        } else {
            listener.error("Collection " + descriptor.getSymbolicPath(name, false) + " does not exist or is not readable.");
        }
    }

    private DeferredPermission restoreResourceEntry(Attributes atts) throws SAXException {

        final String skip = atts.getValue( "skip" );

        //dont process entries which should be skipped
        if(skip != null && !"no".equals(skip)) {
            return new SkippedEntryDeferredPermission();
        }

        final String name = atts.getValue("name");
        if(name == null) {
            throw new SAXException("Resource requires a name attribute");
        }

        final String type;
        if(atts.getValue("type") != null) {
            type = atts.getValue("type");
        } else {
            type = "XMLResource";
        }

        final String owner = atts.getValue("owner");
        final String group = atts.getValue("group");
        final String perms = atts.getValue("mode");

        final String filename;
        if(atts.getValue("filename") != null) {
            filename = atts.getValue("filename");
        } else  {
            filename = name;
        }

        final String mimetype = atts.getValue("mimetype");
        final String created = atts.getValue("created");
        final String modified = atts.getValue("modified");

        final String publicid = atts.getValue("publicid");
        final String systemid = atts.getValue("systemid");
        final String namedoctype = atts.getValue("namedoctype");


        Date date_created = null;
        Date date_modified = null;

        if(created != null) {
            try {
                date_created = (new DateTimeValue(created)).getDate();
            } catch(final XPathException xpe) {
                listener.warn("Illegal creation date. Ignoring date...");
            }
        }
        if(modified != null) {
            try {
                date_modified = (new DateTimeValue(modified)).getDate();
            } catch(final XPathException xpe) {
                listener.warn("Illegal modification date. Ignoring date...");
            }
        }

        final XmldbURI docUri;

        if(version >= STRICT_URI_VERSION) {
            docUri = XmldbURI.create(name);
        } else {
            try {
                docUri = URIUtils.encodeXmldbUriFor(name);
            } catch(final URISyntaxException e) {
                final String msg = "Could not parse document name into a URI: " + e.getMessage();
                listener.error(msg);
                LOG.error(msg, e);
                return new SkippedEntryDeferredPermission();
            }
        }

        final EXistInputSource is = descriptor.getInputSource(filename);
        if(is == null) {
            final String msg = "Failed to restore resource '" + name + "'\nfrom file '" + descriptor.getSymbolicPath( name, false ) + "'.\nReason: Unable to obtain its EXistInputSource";
            listener.warn(msg);
            LOG.error(msg);
            //throw new RuntimeException(msg);
        }

        if (is == null || currentCollection == null) {
            return new SkippedEntryDeferredPermission();
        }

        final DocumentImpl doc;
        try {
            doc = currentCollection.getDocument(broker, docUri);
        } catch (PermissionDeniedException e) {
            listener.error(e.getMessage());
            return new SkippedEntryDeferredPermission();
        }

        //skip if new
        if (doc != null && master.createDocs.contains(doc.getURI().toString())) {
            return new SkippedEntryDeferredPermission();
        }

//        //check diffs
//        try {
//            if (checkHash(broker, doc, calcHash(descriptor.getInputSource(filename))) == EQ) {
//                //equal, check only metadata
//
//                listener.info("Skip content restore because no changes: "+doc.getURI());
//
//                master.skipped.incrementAndGet();
//
//                //TODO: check metadata
//
//                return new SkippedEntryDeferredPermission();
//            }
//        } catch(final Exception e) {
//            listener.warn("Failed to restore resource '" + name + "'\nfrom file '" + descriptor.getSymbolicPath(name, false) + "'.\nReason: " + e.getMessage());
//            LOG.error(e.getMessage(), e);
//            return new SkippedEntryDeferredPermission();
//        }

        final ResourceDeferred deferred;
        if(name.startsWith(XmldbURI.SYSTEM_COLLECTION)) {
            //prevents restore of a backup from changing system collection resource ownership
            deferred = new ResourceDeferred(listener, SecurityManager.SYSTEM, SecurityManager.DBA_GROUP);
        } else {
            deferred = new ResourceDeferred(listener, owner, group);
        }

        deferred.url = currentCollection.getURI().append(docUri);

        deferred.atts = atts;

        deferred.type = type;

        deferred.name = name;
        deferred.docUri = docUri;

        deferred.mimetype = mimetype;
        deferred.namedoctype = namedoctype;

        deferred.perms = perms;

        deferred.date_created = date_created;
        deferred.date_modified = date_modified;

        deferred.publicid = publicid;
        deferred.systemid = systemid;

        deferred.is = is;

        return deferred;
    }

    private void restoreDeletedEntry(Attributes atts) {
        final String name = atts.getValue("name");
        final String type = atts.getValue("type");

        if("collection".equals(type)) {

            try {
                final Collection col = broker.getCollection(currentCollection.getURI().append(name));
                if(col != null) {
                    //delete
                    final TransactionManager txnManager = broker.getDatabase().getTransactionManager();
                    final Txn txn = txnManager.beginTransaction();
                    try {
                        broker.removeCollection(txn, col);
                        txnManager.commit(txn);
                    } catch (final Exception e) {
                        txnManager.abort(txn);

                        listener.warn("Failed to remove deleted collection: " + name + ": " + e.getMessage());
                    } finally {
                        txnManager.close(txn);
                    }
                }
            } catch (final Exception e) {
                listener.warn("Failed to remove deleted collection: " + name + ": " + e.getMessage());
            }

        } else if("resource".equals(type)) {

            try {
                final XmldbURI uri = XmldbURI.create(name);
                final DocumentImpl doc = currentCollection.getDocument(broker, uri);

                if (doc != null) {
                    final TransactionManager txnManager = broker.getDatabase().getTransactionManager();
                    final Txn txn = txnManager.beginTransaction();
                    try {

                        if (doc.getResourceType() == DocumentImpl.BINARY_FILE) {
                            currentCollection.removeBinaryResource(txn, broker, uri);
                        } else {
                            currentCollection.removeXMLResource(txn, broker, uri);
                        }
                        txnManager.commit(txn);

                    } catch(final Exception e) {
                        txnManager.abort(txn);

                        listener.warn("Failed to remove deleted resource: " + name + ": " + e.getMessage());
                    } finally {
                        txnManager.close(txn);
                    }
                }
            } catch (final Exception e) {
                listener.warn("Failed to remove deleted resource: " + name + ": " + e.getMessage());
            }
        }
    }

    private void addACEToDeferredPermissions(Attributes atts) {
        final int index = Integer.parseInt(atts.getValue("index"));
        final ACE_TARGET target = ACE_TARGET.valueOf(atts.getValue("target"));
        final String who = atts.getValue("who");
        final ACE_ACCESS_TYPE access_type = ACE_ACCESS_TYPE.valueOf(atts.getValue("access_type"));
        final int mode = Integer.parseInt(atts.getValue("mode"), 8);

        deferredPermissions.peek().addACE(index, target, who, access_type, mode);
    }

    private void setDeferredPermissions() {
        deferredPermissions.pop().apply();
    }

    class CollectionDeferredPermission extends org.exist.xquery.restore.deferred.AbstractDeferredPermission<Collection> {

        public CollectionDeferredPermission(RestoreListener listener, Collection collection, String owner, String group, Integer mode) {
            super(listener, collection, owner, group, mode);
        }

        @Override
        public void apply() {
            try {
                getTarget().getLock().acquire(LockMode.WRITE_LOCK);

                final TransactionManager txnManager = broker.getDatabase().getTransactionManager();
                final Txn txn = txnManager.beginTransaction();
                try {
                    final Permission permission = getTarget().getPermissions();
                    permission.setOwner(getOwner());
                    permission.setGroup(getGroup());
                    permission.setMode(getMode());
                    if(permission instanceof ACLPermission) {
                        final ACLPermission aclPermission = (ACLPermission)permission;
                        aclPermission.clear();
                        for(final ACEAider ace : getAces()) {
                            aclPermission.addACE(ace.getAccessType(), ace.getTarget(), ace.getWho(), ace.getMode());
                        }
                    }
                    broker.saveCollection(txn, getTarget());

                    txnManager.commit(txn);

                } catch (final Exception xe) {
                    txnManager.abort(txn);

                    throw xe;

                } finally {
                    txnManager.close(txn);
                    getTarget().release(LockMode.WRITE_LOCK);
                }

            } catch (final Exception xe) {
                final String msg = "ERROR: Failed to set permissions on Collection '" + getTarget().getURI() + "'.";
                LOG.error(msg, xe);
                getListener().warn(msg);
            }
        }
    }

    class ResourceDeferred extends org.exist.xquery.restore.deferred.AbstractDeferredPermission<DocumentImpl> {

        public Attributes atts;

        public String type;

        public String name;
        public XmldbURI docUri;

        public String mimetype;
        public String namedoctype;

        public String perms;

        public Date date_created;
        public Date date_modified;

        public String publicid;
        public String systemid;

        public EXistInputSource is;

        public ResourceDeferred(RestoreListener listener, String owner, String group) {
            super(listener, null, owner, group, Permission.DEFAULT_RESOURCE_PERM);
        }

        @Override
        public void apply() {
            try {
                try {
                    mode = Integer.parseInt(perms, 8);
                } catch (NumberFormatException e) {
                    LOG.warn("Document '" + docUri + "' @ '" + currentCollection.getURI() + "' mode is invalid '" + mode + "', use default!");
                }

                try {
                    target = currentCollection.getDocument(broker, docUri);
                } catch (PermissionDeniedException e) {
                    listener.error(currentCollection.getURI().append(docUri).toString() + ": " + e.getMessage());
                }

                if (target != null && (sameLastModified() || rmd.isChecksumSame(listener))) {

                    DocumentMetadata m = getTarget().getMetadata();
                    //DocumentType dt = m.getDocType();

                    Permission p = target.getPermissions();

                    if (date_created.getTime() == m.getCreated()
                            && date_modified.getTime() == m.getLastModified()

                            && p.getMode() == getMode()
                            && p.getOwner() != null && getOwner().equals(p.getOwner().getName())
                            && p.getGroup() != null && getGroup().equals(p.getGroup().getName())) {

                        //unused
//                    if (
//                            (publicid == null && systemid == null && dt == null)
//                            || (dt != null
//                                    && (namedoctype != null && namedoctype.equals(dt.getName()))
//                                    && (publicid != null && publicid.equals(dt.getSystemId()))
//                                    && (systemid != null && systemid.equals(dt.getSystemId()))
//                            )
//                    ) {

                        if (!(p instanceof ACLPermission)) {

                            if ("XMLResource".equals(type)) {
                                master.skippedXML.incrementAndGet();
                            } else {
                                master.skippedBIN.incrementAndGet();
                            }

                            //TODO: check acl for diffs
                            return;
                        }
//                    }
                    }

                    try {
                        final TransactionManager txnManager = broker.getDatabase().getTransactionManager();

                        getTarget().getUpdateLock().acquire(LockMode.WRITE_LOCK);

                        try (Txn txn = txnManager.beginTransaction()) {

                            DocumentMetadata meta = getTarget().getMetadata();
                            meta.setMimeType(mimetype);
                            meta.setCreated(date_created.getTime());
                            meta.setLastModified(date_modified.getTime());

                            if ((publicid != null) || (systemid != null)) {
                                final DocumentType docType = new DocumentTypeImpl(namedoctype, publicid, systemid);
                                meta.setDocType(docType);
//                        } else {
//                            meta.setDocType(null);
                            }

                            final Permission permission = getTarget().getPermissions();
                            permission.setOwner(getOwner());
                            permission.setGroup(getGroup());
                            permission.setMode(getMode());
                            if (permission instanceof ACLPermission) {
                                final ACLPermission aclPermission = (ACLPermission) permission;
                                aclPermission.clear();
                                for (final ACEAider ace : getAces()) {
                                    aclPermission.addACE(ace.getAccessType(), ace.getTarget(), ace.getWho(), ace.getMode());
                                }
                            }
                            broker.storeXMLResource(txn, getTarget());
                            txnManager.commit(txn);

                        } finally {
                            getTarget().getUpdateLock().release(LockMode.WRITE_LOCK);
                        }

                    } catch (final Exception xe) {
                        final String msg = "ERROR: Failed to set permissions on Document '" + getTarget().getURI() + "'.";
                        LOG.error(msg, xe);
                        getListener().warn(msg);
                    }

                } else {
                    restore();
                }
            } finally {
                if (is != null) {
                    is.close();
                }
            }
        }

        private boolean sameLastModified() {
            if (url.toString().contains("/repositories/")) {
                return false;
            }

            DocumentMetadata m = getTarget().getMetadata();

            return date_created.getTime() == m.getCreated() && date_modified.getTime() == m.getLastModified();
        }

        private void restore() {
            try {
                listener.setCurrentResource(name);
                listener.observe(currentCollection);

                final TransactionManager txnManager = broker.getDatabase().getTransactionManager();
                final Txn txn = txnManager.beginTransaction();

                DocumentImpl resource;
                try {
                    if ("XMLResource".equals(type)) {
                        master.updatedXML.incrementAndGet();

                        // store as xml resource

                        final IndexInfo info = currentCollection.validateXMLResource(txn, broker, docUri, is);

                        resource = info.getDocument();
                        final DocumentMetadata meta = resource.getMetadata();
                        meta.setMimeType(mimetype);
                        meta.setCreated(date_created.getTime());
                        meta.setLastModified(date_modified.getTime());

                        if((publicid != null) || (systemid != null)) {
                            final DocumentType docType = new DocumentTypeImpl(namedoctype, publicid, systemid);
                            meta.setDocType(docType);
                        }

                        final Permission permission = resource.getPermissions();
                        permission.setOwner(getOwner());
                        permission.setGroup(getGroup());
                        permission.setMode(getMode());
                        if(permission instanceof ACLPermission) {
                            final ACLPermission aclPermission = (ACLPermission)permission;
                            aclPermission.clear();
                            for(final ACEAider ace : getAces()) {
                                aclPermission.addACE(ace.getAccessType(), ace.getTarget(), ace.getWho(), ace.getMode());
                            }
                        }

                        rh.startRestore(resource, atts);

                        currentCollection.store(txn, broker, info, is, false);

                    } else {
                        master.updatedBIN.incrementAndGet();

                        // store as binary resource
                        resource = currentCollection.validateBinaryResource(txn, broker, docUri, is.getByteStream(), mimetype, is.getByteStreamLength() , date_created, date_modified);

                        rh.startRestore(resource, atts);

                        resource = currentCollection.addBinaryResource(txn, broker, (BinaryDocument)resource, is.getByteStream(), mimetype, is.getByteStreamLength() , date_created, date_modified);
                    }

                    txnManager.commit(txn);

                    //called by rh.endElement
                    //rh.endRestore(resource);

                    listener.restored(name);

                } catch (final Exception e) {
                    txnManager.abort(txn);
                    throw new IOException(e);
                } finally {
                    txnManager.close(txn);
//                if (resource != null)
//                    resource.getUpdateLock().release(Lock.READ_LOCK);
                }

                target = resource;

            } catch(final Exception e) {
                listener.warn("Failed to restore resource '" + name + "'\nfrom file '" + descriptor.getSymbolicPath(name, false) + "'.\nReason: " + e.getMessage());
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private int checkHash(DBBroker broker, DocumentImpl doc, String hash) throws IOException, SAXException {

        if (hash == null) return UNKNOWN;

        MessageDigest digest = messageDigest();

        OutputStream out = new FakeOutputStream();
        DigestOutputStream digestStream = new DigestOutputStream(out, digest);

        switch (doc.getResourceType()) {
            case XML_FILE:

                Writer writerStream = new OutputStreamWriter(
                        digestStream,
                        ENCODING.newEncoder()
                );

                try (BufferedWriter writer = new BufferedWriter(writerStream)) {
                    serializer(broker).serialize(doc, writer);
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

        String calcHash = digestHex(digest);

        if (hash.equals(calcHash)) {
            return EQ;
        }

        return DIFF;
    }

    private String calcHash(EXistInputSource src) throws IOException, SAXException {

        MessageDigest digest = messageDigest();

        OutputStream out = new FakeOutputStream();
        DigestOutputStream digestStream = new DigestOutputStream(out, digest);

        try (InputStream is = src.getByteStream()) {
            IOUtils.copy(is, digestStream);
        }

        return digestHex(digest);
    }

//    private Serializer serializer(DBBroker broker, BufferedWriter writer) throws IOException {
//        // write resource to contentSerializer
//        final SAXSerializer contentSerializer = (SAXSerializer) SerializerPool.getInstance().borrowObject( SAXSerializer.class );
//        contentSerializer.setOutput( writer, defaultOutputProperties );
//
//        final Receiver receiver;
//        if (chainFactory != null) {
//            chainFactory.getLast().setNextInChain(contentSerializer);
//            receiver = chainFactory.getFirst();
//        } else {
//            receiver = contentSerializer;
//        }
//
//        writeXML( doc, receiver );
//
//        SerializerPool.getInstance().returnObject( contentSerializer );
//        writer.flush();
//    }

    protected Serializer serializer(DBBroker broker) {
        Serializer serializer = broker.newSerializer();
        serializer.setUser(broker.getSubject());
        try {
            serializer.setProperty(OutputKeys.INDENT, "no");
            serializer.setProperty(OutputKeys.ENCODING, "UTF-8");
            serializer.setProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            //TODO: is this backup issue?
            //serializer.setProperty(EXistOutputKeys.OUTPUT_DOCTYPE, "yes");
            serializer.setProperty(EXistOutputKeys.EXPAND_XINCLUDES, "no");
            serializer.setProperty(EXistOutputKeys.PROCESS_XSL_PI, "no");

        } catch (Exception e) {}

        return serializer;
    }

    private String digestHex(MessageDigest digest) {
        return Hex.encodeHexString(digest.digest());
    }

    private MessageDigest messageDigest() throws IOException {
        try {
            return MessageDigest.getInstance(MessageDigestAlgorithms.SHA_512);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
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