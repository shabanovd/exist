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
package org.exist.collections;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.exist.EXistException;
import org.exist.collections.triggers.*;
import org.exist.dom.*;
import org.exist.security.Permission;
import org.exist.security.PermissionDeniedException;
import org.exist.security.Subject;
import org.exist.storage.*;
import org.exist.storage.lock.*;
import org.exist.storage.txn.Txn;
import org.exist.util.LockException;
import org.exist.util.SyntaxException;
import org.exist.xmldb.XmldbURI;

/**
 * This class represents a collection in the database. A collection maintains a list of
 * sub-collections and documents, and provides the methods to store/remove resources.
 *
 * Collections are shared between {@link org.exist.storage.DBBroker} instances. The caller
 * is responsible to lock/unlock the collection. Call {@link DBBroker#openCollection(XmldbURI, int)}
 * to get a collection with a read or write lock and {@link #release(int)} to release the lock.
 *
 * @author wolf
 */
public class Collection extends StoredCollectionOperations {

    public static int LENGTH_COLLECTION_ID = 4; //sizeof int

    private final static Logger LOG = Logger.getLogger(Collection.class);
    
    public final static int UNKNOWN_COLLECTION_ID = -1;
    
    public Collection(final DBBroker broker, final XmldbURI path) {
        super(broker.getDatabase(), path);
    }

    /**
     *  Add a new sub-collection to the collection.
     *
     */
    public void addCollection(final DBBroker broker, final Collection child, final boolean isNew) throws PermissionDeniedException {
        checkWritePerms(broker.getSubject());
        
        if(isNew) {
            child.setCreationTime(System.currentTimeMillis());
        }
        
        addCollection(child);
    }

    public boolean hasChildCollection(final DBBroker broker, final XmldbURI path) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());

        return hasCollection(path);
    }
    public List<CollectionEntry> getEntries(final DBBroker broker) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());

        return super.getEntries(broker);
    }

    public CollectionEntry getSubCollectionEntry(final DBBroker broker, final String name) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());

        final XmldbURI subCollectionURI = getURI().append(name);
        final CollectionEntry entry = new SubCollectionEntry(subCollectionURI);
        entry.readMetadata(broker);
        return entry;
    }

    public CollectionEntry getResourceEntry(final DBBroker broker, final XmldbURI name) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());

        final CollectionEntry entry = new DocumentEntry(document(broker, name));
        entry.readMetadata(broker);
        return entry;
    }

    /**
     * Returns true if this is a temporary collection. By default,
     * the temporary collection is in /db/system/temp.
     *
     * @return A boolean where true means the collection is temporary.
     */
    public boolean isTempCollection() {
        return isTempCollection;
    }

    /**
     * Closes the collection, i.e. releases the lock held by
     * the current thread. This is a shortcut for getLock().release().
     */
    public void release(final int mode) {
        getLock().release(mode);
    }

    /**
     * Add a document to the collection.
     *
     * @param  doc
     */
    public void addDocument(final Txn transaction, final DBBroker broker, final DocumentImpl doc) throws PermissionDeniedException {
        addDocument(transaction, broker, doc, null);
    }
    
    /**
     * Removes the document from the internal list of resources, but
     * doesn't delete the document object itself.
     *
     * @param doc
     */
    public void unlinkDocument(final DBBroker broker, final DocumentImpl doc) throws PermissionDeniedException {
        checkWritePerms(broker.getSubject());

        unlinkDocument(doc);
    }

    /**
     *  Return an iterator over all sub-collections.
     *
     * The list of sub-collections is copied first, so modifications
     * via the iterator have no effect.
     *
     * @return An iterator over the collections
     */
    public Iterator<XmldbURI> collectionIterator(final DBBroker broker) throws PermissionDeniedException {
        //XXX: Permission.EXECUTE !!!
        if(!permissions().validate(broker.getSubject(), Permission.READ)) {
            throw new PermissionDeniedException("Permission to list sub-collections denied on " + this.getURI());
        }
        
        try {
            getLock().acquire(Lock.READ_LOCK);
            return collectionsStableIterator();
        } catch(final LockException e) {
            LOG.warn(e.getMessage(), e);
            return null;
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
    }

    /**
     *  Return an iterator over all sub-collections.
     *
     * The list of sub-collections is copied first, so modifications
     * via the iterator have no effect.
     *
     * @return An iterator over the collections
     */
    @Deprecated
    public Iterator<XmldbURI> collectionIteratorNoLock(final DBBroker broker) throws PermissionDeniedException {
        if(!permissions().validate(broker.getSubject(), Permission.READ)) {
            throw new PermissionDeniedException("Permission to list sub-collections denied on " + this.getURI());
        }
        
        return collectionsStableIterator();
    }

    /**
     * Return the collections below this collection
     *
     * @return List
     */
    public List<Collection> getDescendants(final DBBroker broker, final Subject user) throws PermissionDeniedException {
        if(!permissions().validate(broker.getSubject(), Permission.READ)) {
            throw new PermissionDeniedException("Permission to list sub-collections denied on " + this.getURI());
        }
        
        final ArrayList<Collection> collectionList = new ArrayList<Collection>(childCollectionCount());
        try {
            getLock().acquire(Lock.READ_LOCK);
            for(final Iterator<XmldbURI> i = collectionsStableIterator(); i.hasNext(); ) {
                final XmldbURI childName = i.next();
                //TODO : resolve URI !
                final Collection child = broker.getCollection(URI().append(childName));
                if(child.permissions().validate(user, Permission.READ)) {
                    collectionList.add(child);
                    if(child.getChildCollectionCount(broker) > 0) {
                        //Recursive call
                        collectionList.addAll(child.getDescendants(broker, user));
                    }
                }
            }
        } catch(final LockException e) {
            LOG.warn(e.getMessage(), e);
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
        return collectionList;
    }

    public MutableDocumentSet allDocs(final DBBroker broker, final MutableDocumentSet docs, final boolean recursive) throws PermissionDeniedException {
        return allDocs(broker, docs, recursive, null);
    }

    /**
     * Retrieve all documents contained in this collections.
     *
     * If recursive is true, documents from sub-collections are
     * included.
     *
     * @return The set of documents.
     */
    public MutableDocumentSet allDocs(final DBBroker broker, final MutableDocumentSet docs, final boolean recursive, final LockedDocumentMap protectedDocs) throws PermissionDeniedException {
        List<XmldbURI> subColls = null;
        if(permissions().validate(broker.getSubject(), Permission.READ)) {
            try {
                //Acquire a lock on the collection
                getLock().acquire(Lock.READ_LOCK);
                //Add all docs in this collection to the returned set
                getDocuments(broker, docs);
                //Get a list of sub-collection URIs. We will process them
                //after unlocking this collection. otherwise we may deadlock ourselves
                subColls = childCollections();
            } catch(final LockException e) {
                LOG.warn(e.getMessage(), e);
            } finally {
                getLock().release(Lock.READ_LOCK);
            }
        }
        if(recursive && subColls != null) {
            // process the child collections
            for(final XmldbURI childName : subColls) {
                //TODO : resolve URI !
                try {
                    final Collection child = broker.openCollection(URI().appendInternal(childName), Lock.NO_LOCK);
                    //A collection may have been removed in the meantime, so check first
                    if(child != null) {
                        child.allDocs(broker, docs, recursive, protectedDocs);
                    }
                } catch(final PermissionDeniedException pde) {
                    //SKIP to next collection
                    //TODO create an audit log??!
                }
            }
        }
        return docs;
    }

    public DocumentSet allDocs(final DBBroker broker, final MutableDocumentSet docs, final boolean recursive, final LockedDocumentMap lockMap, final int lockType) throws LockException, PermissionDeniedException {
        
        XmldbURI uris[] = null;
        if(permissions().validate(broker.getSubject(), Permission.READ)) {
            try {
                //Acquire a lock on the collection
                getLock().acquire(Lock.READ_LOCK);
                //Add all documents in this collection to the returned set
                getDocuments(broker, docs, lockMap, lockType);
                //Get a list of sub-collection URIs. We will process them
                //after unlocking this collection.
                //otherwise we may deadlock ourselves
                final List<XmldbURI> subColls = childCollections();
                if (subColls != null) {
                    uris = new XmldbURI[subColls.size()];
                    for(int i = 0; i < subColls.size(); i++) {
                        uris[i] = URI().appendInternal(subColls.get(i));
                    }
                }
            } catch(final LockException e) {
                LOG.error(e.getMessage());
                throw e;
            } finally {
                getLock().release(Lock.READ_LOCK);
            }
        }
        
        if(recursive && uris != null) {
            //Process the child collections
            for(int i = 0; i < uris.length; i++) {
                //TODO : resolve URI !
                try {
                    final Collection child = broker.openCollection(uris[i], Lock.NO_LOCK);
                    // a collection may have been removed in the meantime, so check first
                    if(child != null) {
                        child.allDocs(broker, docs, recursive, lockMap, lockType);
                    }
                } catch (final PermissionDeniedException pde) {
                    //SKIP to next collection
                    //TODO create an audit log??!
                }
            }
        }
        return docs;
    }

    /**
     * Add all documents to the specified document set.
     *
     * @param docs
     */
    public DocumentSet getDocuments(final DBBroker broker, final MutableDocumentSet docs) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());
        
        try {
            getLock().acquire(Lock.READ_LOCK);
            docs.addCollection(this);
            addDocumentsToSet(broker.getSubject(), docs);
        } catch(final LockException le) {
            //TODO this should not be caught - it should be thrown - lock errors are bad!!!
            LOG.error(le.getMessage(), le);
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
        
        return docs;
    }

    @Deprecated
    public DocumentSet getDocumentsNoLock(final DBBroker broker, final MutableDocumentSet docs) {
        docs.addCollection(this);
        addDocumentsToSet(broker.getSubject(), docs);
        return docs;
    }

    public DocumentSet getDocuments(final DBBroker broker, final MutableDocumentSet docs, final LockedDocumentMap lockMap, final int lockType) throws LockException, PermissionDeniedException {
        checkReadPerms(broker.getSubject());
        
        try {
            getLock().acquire(Lock.READ_LOCK);
            docs.addCollection(this);
            addDocumentsToSet(broker.getSubject(), docs, lockMap, lockType);
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
        return docs;
    }

    /**
     * Return the number of child-collections managed by this collection.
     *
     * @return The childCollectionCount value
     */
    public int getChildCollectionCount(final DBBroker broker) throws PermissionDeniedException {
    
        checkReadPerms(broker.getSubject());
        
        try {
            getLock().acquire(Lock.READ_LOCK);
            return childCollectionCount();
        } catch(final LockException e) {
            LOG.warn(e.getMessage(), e);
            return 0;
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
    }

    /**
     * Determines if this Collection has any documents, or sub-collections
     */
    @Deprecated
    public boolean isEmptyNoLock(final DBBroker broker) throws PermissionDeniedException {
        
        checkReadPerms(broker.getSubject());
        
        return isEmpty();
    }

    /**
     * Determines if this Collection has any documents, or sub-collections
     */
    public boolean isEmpty(final DBBroker broker) throws PermissionDeniedException {
        
        checkReadPerms(broker.getSubject());
        
        try {
            getLock().acquire(Lock.READ_LOCK);
            
            return isEmpty();
        } catch(final LockException e) {
            LOG.warn(e.getMessage(), e);
            return false;
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
    }

   /**
     * Get a child resource as identified by path. This method doesn't put
     * a lock on the document nor does it recognize locks held by other threads.
     * There's no guarantee that the document still exists when accessing it.
     *
     * @param  broker
     * @param  path  The name of the document (without collection path)
     * @return the document
     */
    public DocumentImpl getDocument(final DBBroker broker, final XmldbURI path) throws PermissionDeniedException {
        try {
            getLock().acquire(Lock.READ_LOCK);
            
            return document(broker, path);
            
        } catch(final LockException e) {
            LOG.warn(e.getMessage(), e);
            return null;
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
    }

    /**
     * Retrieve a child resource after putting a read lock on it. With this method,
     * access to the received document object is safe.
     *
     * @deprecated Use getDocumentWithLock(DBBroker broker, XmldbURI uri, int lockMode)
     * @param broker
     * @param name
     * @return The document that was locked.
     * @throws LockException
     */
    @Deprecated
    public DocumentImpl getDocumentWithLock(final DBBroker broker, final XmldbURI name) throws LockException, PermissionDeniedException {
    	return getDocumentWithLock(broker,name,Lock.READ_LOCK);
    }

    /**
     * Retrieve a child resource after putting a read lock on it. With this method,
     * access to the received document object is safe.
     *
     * @param broker
     * @param uri
     * @param lockMode
     * @return The document that was locked.
     * @throws LockException
     */
    public DocumentImpl getDocumentWithLock(final DBBroker broker, final XmldbURI uri, final int lockMode) throws LockException, PermissionDeniedException {
        try {
            getLock().acquire(Lock.READ_LOCK);
            
            final DocumentImpl doc = document(broker, uri);
            
            if(doc != null) {
                if(!doc.getPermissions().validate(broker.getSubject(), Permission.READ)) {
                    throw new PermissionDeniedException("Permission denied to read document: " + uri.toString());
                }
            	doc.getUpdateLock().acquire(lockMode);
            }
            return doc;
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
    }

    @Deprecated
    public DocumentImpl getDocumentNoLock(final DBBroker broker, final String rawPath) throws PermissionDeniedException {
        DocumentImpl doc;
        try {
            doc = document(broker, XmldbURI.xmldbUriFor(rawPath));
        } catch (URISyntaxException e) {
            throw new PermissionDeniedException(e);
        }
        if(doc != null) {
            if(!doc.getPermissions().validate(broker.getSubject(), Permission.READ)) {
                throw new PermissionDeniedException("Permission denied to read document: " + rawPath);
            }
        }
        return doc;
    }

    /**
     * Release any locks held on the document.
     * @deprecated Use releaseDocument(DocumentImpl doc, int mode)
     * @param doc
     */
    @Deprecated
    public void releaseDocument(final DocumentImpl doc) {
        if(doc != null) {
            doc.getUpdateLock().release(Lock.READ_LOCK);
        }
    }

    /**
     * Release any locks held on the document.
     *
     * @param doc
     */
    public void releaseDocument(final DocumentImpl doc, final int mode) {
        if(doc != null) {
            doc.getUpdateLock().release(mode);
        }
    }

    /**
     * Returns the number of documents in this collection.
     *
     * @return The documentCount value
     */
    public int getDocumentCount(final DBBroker broker) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());
        
        try {
            getLock().acquire(Lock.READ_LOCK);
            
            return documentsCount();
        } catch(final LockException e) {
            LOG.warn(e.getMessage(), e);
            return 0;
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
    }

    @Deprecated
    public int getDocumentCountNoLock(final DBBroker broker) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());

        return documentsCount();
    }

    /**
     * Gets the permissions attribute of the Collection object
     *
     * @return The permissions value
     */
    final public Permission getPermissions() {
        try {
            getLock().acquire(Lock.READ_LOCK);
            return permissions();
        } catch(final LockException e) {
            LOG.warn(e.getMessage(), e);
            return permissions();
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
    }

    @Deprecated
    final public Permission getPermissionsNoLock() {
        return permissions();
    }
    
    /**
     * Check if the collection has a child document.
     *
     * @param  uri  the name (without path) of the document
     * @return A value of true when the collection has the document identified.
     */
    public boolean hasDocument(final DBBroker broker, final XmldbURI uri) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());
        
        return hasDocument(uri);
    }

    /**
     * Check if the collection has a sub-collection.
     *
     * @param  name  the name of the subcollection (without path).
     * @return A value of true when the subcollection exists.
     */
    public boolean hasSubcollection(final DBBroker broker, final XmldbURI name) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());
        
        try {
            getLock().acquire(Lock.READ_LOCK);
            return hasCollection(name);
        } catch(final LockException e) {
            LOG.warn(e.getMessage(), e);
            //TODO : ouch ! Should we return at any price ? Xithout even logging ? -pb
            return hasCollection(name);
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
    }

    @Deprecated
    public boolean hasSubcollectionNoLock(final DBBroker broker, final XmldbURI name) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());
        
        return hasCollection(name);
    }

    /**
     * Returns an iterator on the child-documents in this collection.
     *
     * @return A iterator of all the documents in the collection.
     */
    public Iterator<DocumentImpl> iterator(final DBBroker broker) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());
        
        return getDocuments(broker, new DefaultDocumentSet()).getDocumentIterator();
    }

    @Deprecated
    public Iterator<DocumentImpl> iteratorNoLock(final DBBroker broker) throws PermissionDeniedException {
        checkReadPerms(broker.getSubject());
        
        return getDocumentsNoLock(broker, new DefaultDocumentSet()).getDocumentIterator();
    }

    /**
     * Remove the specified sub-collection.
     *
     * @param  name  Description of the Parameter
     */
    public void removeCollection(final DBBroker broker, final XmldbURI name) throws LockException, PermissionDeniedException {
        checkWritePerms(broker.getSubject());
        
        getLock().acquire(Lock.WRITE_LOCK);
        try {

            removeCollection(name);

        } finally {
            getLock().release(Lock.WRITE_LOCK);
        }
    }

    public void removeBinaryResource(final Txn transaction, final DBBroker broker, final XmldbURI uri) throws PermissionDeniedException, LockException, TriggerException {
        checkWritePerms(broker.getSubject());
        
        try {
            getLock().acquire(Lock.READ_LOCK);
            final DocumentImpl doc = getDocument(broker, uri);
            
            if(doc.isLockedForWrite()) {
                throw new PermissionDeniedException("Document " + doc.getFileURI() + " is locked for write");
            }
            
            removeBinaryResource(transaction, broker, doc);
        } finally {
            getLock().release(Lock.READ_LOCK);
        }
    }

    // Blob
    @Deprecated
    public BinaryDocument addBinaryResource(final Txn transaction, final DBBroker broker, final XmldbURI docUri, final byte[] data, final String mimeType) throws EXistException, PermissionDeniedException, LockException, TriggerException,IOException {
        return addBinaryResource(transaction, broker, docUri, data, mimeType, null, null);
    }

    // Blob
    @Deprecated
    public BinaryDocument addBinaryResource(final Txn transaction, final DBBroker broker, final XmldbURI docUri, final byte[] data, final String mimeType, final Date created, final Date modified) throws EXistException, PermissionDeniedException, LockException, TriggerException,IOException {
        return addBinaryResource(transaction, broker, docUri, new ByteArrayInputStream(data), mimeType, data.length, created, modified);
    }

    // Streaming
    public BinaryDocument addBinaryResource(final Txn transaction, final DBBroker broker, final XmldbURI docUri, final InputStream is, final String mimeType, final long size) throws EXistException, PermissionDeniedException, LockException, TriggerException,IOException {
        return addBinaryResource(transaction, broker, docUri, is, mimeType, size, null, null);
    }

    // Streaming
    public BinaryDocument addBinaryResource(final Txn transaction, final DBBroker broker, final XmldbURI docUri, final InputStream is, final String mimeType, final long size, final Date created, final Date modified) throws EXistException, PermissionDeniedException, LockException, TriggerException, IOException {
        final BinaryDocument blob = new BinaryDocument(broker.getBrokerPool(), this, docUri);
        
        return addBinaryResource(transaction, broker, blob, is, mimeType, size, created, modified);
    }

    public BinaryDocument validateBinaryResource(final Txn transaction, final DBBroker broker, final XmldbURI docUri, final InputStream is, final String mimeType, final long size, final Date created, final Date modified) throws PermissionDeniedException, LockException, TriggerException, IOException {
        return new BinaryDocument(broker.getBrokerPool(), this, docUri);
    }

    public void setPermissions(final int mode) throws LockException, PermissionDeniedException {
        getLock().acquire(Lock.WRITE_LOCK);
        try {
            permissions().setMode(mode);
        } finally {
            getLock().release(Lock.WRITE_LOCK);
        }
    }

    @Deprecated
    public void setPermissions(final String mode) throws SyntaxException, LockException, PermissionDeniedException {
        getLock().acquire(Lock.WRITE_LOCK);
        try {
            permissions().setMode(mode);
        } finally {
            getLock().release(Lock.WRITE_LOCK);
        }
    }

    /**
     * Set permissions for the collection.
     *
     * @param permissions
     *
     * @deprecated This function is considered a security problem
     * and should be removed, move code to copyOf or Constructor
     */
    @Deprecated
    public void setPermissions(final Permission permissions) throws LockException {
        getLock().acquire(Lock.WRITE_LOCK);
        try {
            setPermissions(permissions);
        } finally {
            getLock().release(Lock.WRITE_LOCK);
        }
    }

    /**
     * (Make private?)
     * @param broker
     */
    public IndexSpec getIndexConfiguration(final DBBroker broker) {
        final CollectionConfiguration conf = getConfiguration(broker);
        //If the collection has its own config...
        if (conf == null) {
            return broker.getIndexConfiguration();
        }
        //... otherwise return the general config (the broker's one)
        return conf.getIndexConfiguration();
    }

    public GeneralRangeIndexSpec getIndexByPathConfiguration(final DBBroker broker, final NodePath path) {
        final IndexSpec idxSpec = getIndexConfiguration(broker);
        return (idxSpec == null) ? null : idxSpec.getIndexByPath(path);
    }

    public QNameRangeIndexSpec getIndexByQNameConfiguration(final DBBroker broker, final QName qname) {
        final IndexSpec idxSpec = getIndexConfiguration(broker);
        return (idxSpec == null) ? null : idxSpec.getIndexByQName(qname);
    }
}
