/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2013 The eXist Project
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
package org.exist.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.exist.Database;
import org.exist.EXistException;
import org.exist.collections.Collection;
import org.exist.collections.CollectionCache;
import org.exist.collections.CollectionConfiguration;
import org.exist.collections.CollectionConfigurationException;
import org.exist.collections.CollectionConfigurationManager;
import org.exist.collections.triggers.CollectionTrigger;
import org.exist.collections.triggers.CollectionTriggersVisitor;
import org.exist.collections.triggers.TriggerException;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.dom.StoredNode;
import org.exist.security.Permission;
import org.exist.security.PermissionDeniedException;
import org.exist.security.Subject;
import org.exist.storage.NativeBroker.NodeRef;
import org.exist.storage.btree.BTreeException;
import org.exist.storage.btree.IndexQuery;
import org.exist.storage.btree.Value;
import org.exist.storage.btree.Paged.Page;
import org.exist.storage.dom.DOMFile;
import org.exist.storage.dom.DOMTransaction;
import org.exist.storage.index.BFile;
import org.exist.storage.index.CollectionStore;
import org.exist.storage.io.VariableByteInput;
import org.exist.storage.journal.Journal;
import org.exist.storage.journal.Loggable;
import org.exist.storage.lock.Lock;
import org.exist.storage.txn.TransactionException;
import org.exist.storage.txn.Txn;
import org.exist.util.ByteConversion;
import org.exist.util.LockException;
import org.exist.util.ReadOnlyException;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.TerminatedException;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public abstract class StoredCollectionOperations extends Stored {
    
    Database db;
    
    public StoredCollectionOperations(Database db, XmldbURI path) {
        super(path);
        
        this.db = db;
    }
    
    public Collection getCollection(XmldbURI uri) throws PermissionDeniedException {
        return openCollection(uri, Lock.NO_LOCK);
    }

    public Collection openCollection(XmldbURI uri, int lockMode) throws PermissionDeniedException {
        return openCollection((NativeBroker)db.getActiveBroker(), uri, BFile.UNKNOWN_ADDRESS, lockMode);
    }
    
    private static Collection readCollection(DBBroker broker, CollectionStore collectionsDb, CollectionCache collectionsCache, XmldbURI uri, long addr) throws PermissionDeniedException {
        final Lock lock = collectionsDb.getLock();
        try {
            lock.acquire(Lock.READ_LOCK);
            
            VariableByteInput is;
            
            if (addr == BFile.UNKNOWN_ADDRESS) {
                final Value key = new CollectionStore.CollectionKey(uri.toString());
                is = collectionsDb.getAsStream(key);
            } else {
                is = collectionsDb.getAsStream(addr);
            }
            
            if (is == null) {
                return null;
            }
            
            Collection collection = new Collection(broker, uri);
            collection.read(broker, is);
            
            collectionsCache.add(collection);
            
            return collection;
            
        //TODO : rethrow exceptions ? -pb
        } catch (final LockException e) {
            LOG.warn("Failed to acquire lock on " + collectionsDb.getFile().getName());
            return null;
        } catch (final IOException e) {
            LOG.error(e.getMessage(), e);
            return null;
        } finally {
            lock.release(Lock.READ_LOCK);
        }
    }
    
    /**
     *  Get collection object. If the collection does not exist, null is
     *  returned.
     *
     *@param  uri  collection URI
     *@return       The collection value
     */
    protected static final Collection openCollection(NativeBroker broker, XmldbURI uri, long addr, int lockMode) throws PermissionDeniedException {
        XmldbURI path = broker.prepend(uri.toCollectionPathURI());

        Stored collection = null;
        
        final CollectionCache collectionsCache = broker.getDatabase().getCollectionsCache();  
        synchronized(collectionsCache) {
            collection = collectionsCache.get(path);
            if (collection == null) {
                collection = readCollection(broker, broker.getDatabase().collectionStore(), collectionsCache, path, addr);
            } else {
                if (!collection.getURI().equalsInternal(path)) {
                    LOG.error("The collection received from the cache is not the requested: " + path + "; received: " + collection.getURI());
                }
                
                collectionsCache.add(collection);
            }
        }
        
        if (collection == null)
            return null;

        if(!collection.permissions().validate(broker.getSubject(), Permission.EXECUTE)) {
            throw new PermissionDeniedException("Permission denied to open collection: " + collection.getURI().toString() + " by " + broker.getSubject().getName());
        }

        //Important : 
        //This code must remain outside of the synchonized block
        //because another thread may already own a lock on the collection
        //This would result in a deadlock... until the time-out raises the Exception
        //TODO : make an attempt to an immediate lock ?
        //TODO : manage a collection of requests for locks ?
        //TODO : another yet smarter solution ?
        if(lockMode != Lock.NO_LOCK) {
            try {
                collection.getLock().acquire(lockMode);
            } catch (final LockException e) {
                LOG.warn("Failed to acquire lock on collection '" + path + "'");
            }
        }
        return (Collection) collection;
    }
    
    public static Collection getOrCreateCollection(NativeBroker broker, Txn txn, XmldbURI uri) throws PermissionDeniedException, IOException, TriggerException {
        
        uri = broker.prepend(uri.normalizeCollectionPath());
        
        final CollectionCache collectionsCache = broker.getDatabase().getCollectionsCache();
        
        synchronized(collectionsCache) {
            try {
                //TODO : resolve URIs !
                final XmldbURI[] segments = uri.getPathSegments();

                XmldbURI path = XmldbURI.ROOT_COLLECTION_URI;

                Collection current = openCollection(broker, path, BFile.UNKNOWN_ADDRESS, Lock.NO_LOCK);
                if (current == null) {
                    current = createCollection(broker.getDatabase(), txn, null, path);

                    //import an initial collection configuration
                    initialCollectionConfiguration(broker, txn, current);
                }
                
                for(int i=1; i < segments.length; i++) {
                    
                    final XmldbURI temp = segments[i];
                    path = path.append(temp);
                    
                    if(current.hasChildCollection(broker, temp)) {
                        current = openCollection(broker, path, BFile.UNKNOWN_ADDRESS, Lock.NO_LOCK);
                        if (current == null) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Collection '" + path + "' not found!");
                            }
                        }
                    } else {
                        current = createCollection(broker.getDatabase(), txn, current, path);
                    }
                }
                return current;
            } catch (final LockException e) {
                return null;
            }                
        }
    }    

    private final static String INIT_COLLECTION_CONFIG = "collection.xconf.init";
    
    private static final String readInitCollectionConfig(File dir) {
        final File fInitCollectionConfig = new File(dir, INIT_COLLECTION_CONFIG);
        if(fInitCollectionConfig.exists() && fInitCollectionConfig.isFile()) {
            
            InputStream is = null;
            try {
                final StringBuilder initCollectionConfig = new StringBuilder();
                
                is = new FileInputStream(fInitCollectionConfig);
                int read = -1;
                final byte buf[] = new byte[1024];
                while((read = is.read(buf)) != -1) {
                    initCollectionConfig.append(new String(buf, 0, read));
                }
                
                return initCollectionConfig.toString();
            } catch(final IOException ioe) {
                LOG.error(ioe.getMessage(), ioe);
            } finally {
                if(is != null) {
                    try {
                        is.close();
                    } catch(final IOException ioe) {
                        LOG.warn(ioe.getMessage(), ioe);
                    }
                }
            }
                    
        };
        return null;
    }
    
    protected void checkPermsForCollectionWrite(Subject subject, XmldbURI uri) throws PermissionDeniedException {
        if (db.isReadOnly()) {
            throw new PermissionDeniedException(Database.IS_READ_ONLY);
        }
        
        final Permission perms = permissions();
        
        if(!perms.validate(subject, Permission.WRITE)) {
            LOG.error("Permission denied to create collection '" + uri + "'");
            throw new PermissionDeniedException("Account '"+ subject.getName() + "' not allowed to write to collection '" + URI() + "'");
        }
        
        if (!perms.validate(subject, Permission.EXECUTE)) {
            LOG.error("Permission denied to create collection '" + uri + "'");
            throw new PermissionDeniedException("Account '"+ subject.getName() + "' not allowed to execute to collection '" + URI() + "'");
        }
        
        if (hasDocument(uri.lastSegment())) {
            LOG.error("Collection '" + URI() + "' have document '" + uri.lastSegment() + "'");
            throw new PermissionDeniedException("Collection '" + URI() + "' have document '" + uri.lastSegment() + "'.");
        }
    }
    
    private static Collection createCollection(Database db, Txn txn, Collection parent, XmldbURI uri) throws PermissionDeniedException, TriggerException, LockException, IOException {
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating collection '" + uri + "'");
        }
        
        DBBroker broker = db.getActiveBroker();
        
        if (parent != null) {
            parent.checkPermsForCollectionWrite(broker.getSubject(), uri);
        }
        
        CollectionTriggersVisitor triggersVisitor = null;
        if(parent != null) {
            final CollectionConfiguration conf = parent.getConfiguration(broker);
            if(conf != null) {
                triggersVisitor = conf.getCollectionTriggerProxies().instantiateVisitor(broker);
            }
        }

        final CollectionTrigger trigger = db.getCollectionTrigger();
        trigger.beforeCreateCollection(broker, txn, uri);
        if (triggersVisitor != null) triggersVisitor.beforeCreateCollection(broker, txn, uri);

        
        Collection col = new Collection(broker, uri);
        
        col.setId(getNextCollectionId(broker, txn));
        col.setCreationTime(System.currentTimeMillis());
        
        if(txn != null) {
            txn.acquireLock(col.getLock(), Lock.WRITE_LOCK);
        }
        
        //TODO : acquire lock manually if transaction is null ?
        if (parent != null) {
            parent.addCollection(broker, col, true);
        }

        if (parent != null) {
            parent.saveCollection(broker, txn);
        }
        col.saveCollection(broker, txn);
        
        trigger.afterCreateCollection(broker, txn, (Collection)col);
        if (triggersVisitor != null) triggersVisitor.afterCreateCollection(broker, txn, (Collection)col);
        
        return col;
    }
    
    private static void initialCollectionConfiguration(DBBroker broker, Txn txn, Collection col) {
        try {
            Database db = broker.getDatabase();
            
            final String initCollectionConfig = readInitCollectionConfig(db.getConfiguration().getExistHome());
            if(initCollectionConfig != null) {
                CollectionConfigurationManager collectionConfigurationManager = db.getConfigurationManager();
                if(collectionConfigurationManager == null) {
                    //might not yet have been initialised
                    ((BrokerPool)db).initCollectionConfigurationManager(broker);
                    collectionConfigurationManager = db.getConfigurationManager();
                }
                
                if(collectionConfigurationManager != null) {
                    collectionConfigurationManager.addConfiguration(txn, broker, col, initCollectionConfig);
                }
            }
        } catch(final CollectionConfigurationException cce) {
            LOG.error("Could not load initial collection configuration for /db: " + cce.getMessage(), cce);
        }
    }


    /**
     * Checks all permissions in the tree to ensure that a copy operation will succeed
     */
    private final void checkPermissionsForCopy(final DBBroker broker, final Subject subject, final Collection src, final XmldbURI destUri) throws PermissionDeniedException, LockException {
        
        if(!src.permissions().validate(subject, Permission.EXECUTE | Permission.READ)) {
            throw new PermissionDeniedException("Permission denied to copy collection " + src.getURI() + " by " + subject.getName());
        }
        
        
        final Collection dest = getCollection(destUri);
        final XmldbURI newDestUri = destUri.append(src.getURI().lastSegment());
        final Collection newDest = getCollection(newDestUri);
        
        if(dest != null) {
            if(!dest.permissions().validate(subject, Permission.EXECUTE | Permission.WRITE | Permission.READ)) {
                throw new PermissionDeniedException("Permission denied to copy collection " + src.getURI() + " to " + dest.getURI() + " by " + subject.getName());
            }
            
            if(newDest != null) {
                if(!dest.permissions().validate(subject, Permission.EXECUTE | Permission.READ)) {
                    throw new PermissionDeniedException("Permission denied to copy collection " + src.getURI() + " to " + dest.getURI() + " by " + subject.getName());
                }
                
                if(newDest.isEmpty()) {
                    if(!dest.permissions().validate(subject, Permission.WRITE)) {
                        throw new PermissionDeniedException("Permission denied to copy collection " + src.getURI() + " to " + dest.getURI() + " by " + subject.getName());
                    }
                }
            }
        }
        
        for(final Iterator<DocumentImpl> itSrcSubDoc = src.iterator(broker); itSrcSubDoc.hasNext();) {
            final DocumentImpl srcSubDoc = itSrcSubDoc.next();
            if(!srcSubDoc.getPermissions().validate(subject, Permission.READ)) {
                throw new PermissionDeniedException("Permission denied to copy collection " + src.getURI() + " for resource " + srcSubDoc.getURI() + " by " + subject.getName());
            }
            
            if(newDest != null && !newDest.isEmpty()) {
                final DocumentImpl newDestSubDoc = newDest.getDocument(broker, srcSubDoc.getFileURI()); //TODO check this uri is just the filename!
                if(newDestSubDoc != null) {
                    if(!newDestSubDoc.getPermissions().validate(subject, Permission.WRITE)) {
                        throw new PermissionDeniedException("Permission denied to copy collection " + src.getURI() + " for resource " + newDestSubDoc.getURI() + " by " + subject.getName());
                    }
                } else {
                    if(!dest.permissions().validate(subject, Permission.WRITE)) {
                        throw new PermissionDeniedException("Permission denied to copy collection " + src.getURI() + " to " + dest.getURI() + " by " + subject.getName());
                    }
                }
            }
        }
        
        for(final Iterator<XmldbURI> itSrcSubColUri = src.collectionIterator(broker); itSrcSubColUri.hasNext();) {
            final XmldbURI srcSubColUri = itSrcSubColUri.next();
            final Collection srcSubCol = getCollection(src.getURI().append(srcSubColUri));
            
            checkPermissionsForCopy(broker, subject, srcSubCol, newDestUri);
        }
    }
    
    public void copyCollection(final Txn transaction, final Collection destination, final XmldbURI newName) throws PermissionDeniedException, LockException, IOException, TriggerException, EXistException {
        if(db.isReadOnly()) {
            throw new PermissionDeniedException(Database.IS_READ_ONLY);
        }
        
        //TODO : resolve URIs !!!
        if(newName != null && newName.numSegments() != 1) {
            throw new PermissionDeniedException("New collection name must have one segment!");
        }
        
        final XmldbURI srcURI = getURI();
        final XmldbURI dstURI = destination.getURI().append(newName);

        if(getURI().equals(dstURI)) {
            throw new PermissionDeniedException("Cannot move collection to itself '"+getURI()+"'.");
        }
        if(getId() == destination.getId()) {
            throw new PermissionDeniedException("Cannot move collection to itself '"+getURI()+"'.");
        }
        
        final DBBroker broker = db.getActiveBroker();
        
        final CollectionCache collectionsCache = db.getCollectionsCache();
        synchronized(collectionsCache) {
            CollectionStore collectionsDb = db.collectionStore();
            final Lock lock = collectionsDb.getLock();
            try {
                db.getProcessMonitor().startJob(ProcessMonitor.ACTION_COPY_COLLECTION, getURI());
                lock.acquire(Lock.WRITE_LOCK);
                
                final XmldbURI parentName = getParentURI();
                final Collection parent = parentName == null ? (Collection)this : getCollection(parentName);

                final CollectionTriggersVisitor triggersVisitor = parent.getConfiguration(broker).getCollectionTriggerProxies().instantiateVisitor(broker);
                triggersVisitor.beforeCopyCollection(broker, transaction, (Collection)this, dstURI);

                //atomically check all permissions in the tree to ensure a copy operation will succeed before starting copying
                checkPermissionsForCopy(broker, broker.getSubject(), (Collection)this, destination.getURI());
                
                final Collection newCollection = doCopyCollection(transaction, (Collection)this, destination, newName);

                triggersVisitor.afterCopyCollection(broker, transaction, newCollection, srcURI);
            } finally {
                lock.release(Lock.WRITE_LOCK);
                db.getProcessMonitor().endJob();
            }
        }
    }

    private Collection doCopyCollection(final Txn transaction, final Collection collection, final Collection destination, XmldbURI newName) throws PermissionDeniedException, IOException, EXistException, TriggerException, LockException {
        
        final NativeBroker broker = (NativeBroker) db.getActiveBroker();
        
        if(newName == null)
            {newName = collection.getURI().lastSegment();}

        newName = destination.getURI().append(newName);
        
        if (LOG.isDebugEnabled())
                {LOG.debug("Copying collection to '" + newName + "'");}
        
        final Collection destCollection = getOrCreateCollection((NativeBroker)broker, transaction, newName);
        for(final DocumentImpl child : collection.documents()) {

            if (LOG.isDebugEnabled())
                {LOG.debug("Copying resource: '" + child.getURI() + "'");}
            
            final XmldbURI newUri = destCollection.getURI().append(child.getFileURI());
            db.getDocumentTrigger().beforeCopyDocument(broker, transaction, child, newUri);
            
            DocumentImpl createdDoc;
            if (child.getResourceType() == DocumentImpl.XML_FILE) {
                //TODO : put a lock on newDoc ?
                final DocumentImpl newDoc = new DocumentImpl(db, destCollection, child.getFileURI());
                newDoc.copyOf(child);
                newDoc.setDocId(getNextResourceId(broker, transaction));
                broker.copyXMLResource(transaction, child, newDoc);
                broker.storeXMLResource(transaction, newDoc);
                destCollection.addDocument(transaction, broker, newDoc);
                
                createdDoc = newDoc;
            } else {
                final BinaryDocument newDoc = new BinaryDocument(db, destCollection, child.getFileURI());
                newDoc.copyOf(child);
                newDoc.setDocId(getNextResourceId(broker, transaction));
                
                InputStream is = null;
                try {
                    is = broker.getBinaryResource((BinaryDocument)child);
                    broker.storeBinaryResource(transaction,newDoc,is);
                } finally {
                    is.close();
                }
                broker.storeXMLResource(transaction, newDoc);
                destCollection.addDocument(transaction, broker, newDoc);

                createdDoc = newDoc;
            }
            
            db.getDocumentTrigger().afterCopyDocument(broker, transaction, createdDoc, child.getURI());
        }
        destCollection.saveCollection(broker, transaction);
        
        final XmldbURI name = collection.getURI();

        for (XmldbURI childName : collection.childCollections()) {

            //TODO : resolve URIs ! collection.getURI().resolve(childName)
            final Collection child = openCollection(name.append(childName), Lock.WRITE_LOCK);
            if(child == null) {
                LOG.warn("Child collection '" + childName + "' not found");
            } else {
                try {
                    doCopyCollection(transaction, child, destCollection, childName);
                } finally {
                    child.release(Lock.WRITE_LOCK);
                }
            }
        }
        destCollection.saveCollection(broker, transaction);
        destination.saveCollection(broker, transaction);
        
        return destCollection;
    }
    
    public void moveCollection(Txn transaction, Collection destination, XmldbURI newName)  throws PermissionDeniedException, LockException, IOException, TriggerException {
        
        if(db.isReadOnly()) {
            throw new PermissionDeniedException(Database.IS_READ_ONLY);
        }
        
        final File fsDir = ((BrokerPool)db).fsDir();
        final File fsBackupDir = ((BrokerPool)db).fsBackupDir();
        
        final NativeBroker broker = (NativeBroker) db.getActiveBroker();
        final Subject subject = broker.getSubject();
        
        if(newName != null && newName.numSegments() != 1) {
            throw new PermissionDeniedException("New collection name must have one segment!");
        }
        
        if(getId() == destination.getId()) {
            throw new PermissionDeniedException("Cannot move collection to itself '"+URI()+"'.");
        }
        if(getURI().equals(destination.getURI().append(newName))) {
            throw new PermissionDeniedException("Cannot move collection to itself '"+URI()+"'.");
        }
        if(getURI().equals(XmldbURI.ROOT_COLLECTION_URI)) {
            throw new PermissionDeniedException("Cannot move the db root collection");
        }
        
        final XmldbURI parentName = getParentURI();
        final Collection parent = parentName == null ? (Collection)this : getCollection(parentName);
        if(!parent.permissions().validate(subject, Permission.WRITE | Permission.EXECUTE)) {
            throw new PermissionDeniedException("Account "+subject.getName()+" have insufficient privileges on collection " + parent.getURI() + " to move collection " + URI());
        }
        
        if(!permissions().validate(subject, Permission.WRITE)) {
            throw new PermissionDeniedException("Account "+subject.getName()+" have insufficient privileges on collection to move collection " + URI());
        }
        
        if(!destination.permissions().validate(subject, Permission.WRITE | Permission.EXECUTE)) {
            throw new PermissionDeniedException("Account "+subject.getName()+" have insufficient privileges on collection " + parent.getURI() + " to move collection " + URI());
        }
        
        /*
         * If replacing another collection in the move i.e. /db/col1/A -> /db/col2 (where /db/col2/A exists)
         * we have to make sure the permissions to remove /db/col2/A are okay!
         * 
         * So we must call removeCollection on /db/col2/A
         * Which will ensure that collection can be removed and then remove it.
         */
        final XmldbURI movedToCollectionUri = destination.getURI().append(newName);
        final Collection existingMovedToCollection = getCollection(movedToCollectionUri);
        if(existingMovedToCollection != null) {
            existingMovedToCollection.removeCollection(transaction);
        }
        
        db.getProcessMonitor().startJob(ProcessMonitor.ACTION_MOVE_COLLECTION, URI());
        
        try {
                
            final XmldbURI srcURI = URI();
            final XmldbURI dstURI = destination.getURI().append(newName);

            final CollectionTriggersVisitor triggersVisitor = parent.getConfiguration(broker).getCollectionTriggerProxies().instantiateVisitor(broker);
            triggersVisitor.beforeMoveCollection(broker, transaction, (Collection)this, dstURI);
            
            // sourceDir must be known in advance, because once moveCollectionRecursive
            // is called, both collection and destination can point to the same resource
            final File fsSourceDir = broker.getCollectionFile(fsDir, URI(),false);
        
            // Need to move each collection in the source tree individually, so recurse.
            moveCollectionRecursive(broker, transaction, (Collection)this, destination, newName);
            
            // For binary resources, though, just move the top level directory and all descendants come with it.
            moveBinaryFork(transaction, fsSourceDir, destination, newName);
            
            triggersVisitor.afterMoveCollection(broker, transaction, (Collection)this, srcURI);

        } finally {
            db.getProcessMonitor().endJob();
        }
    }
    
    private void moveBinaryFork(Txn transaction, File sourceDir, Collection destination, XmldbURI newName) throws IOException {
        final NativeBroker broker = (NativeBroker) db.getActiveBroker();

        final Journal logManager = db.getTransactionManager().getJournal();
        
        final File fsDir = ((BrokerPool)db).fsDir();
        final File fsBackupDir = ((BrokerPool)db).fsBackupDir();
        
        final File targetDir = broker.getCollectionFile(fsDir,destination.getURI().append(newName),false);
        if (sourceDir.exists()) {
            if(targetDir.exists()) {
                final File targetDelDir = broker.getCollectionFile(fsBackupDir,transaction,destination.getURI().append(newName),true);
                targetDelDir.getParentFile().mkdirs();
                if (targetDir.renameTo(targetDelDir)) {
                    final Loggable loggable = new RenameBinaryLoggable(db.getActiveBroker(),transaction,targetDir,targetDelDir);
                    try {
                        logManager.writeToLog(loggable);
                    } catch (final TransactionException e) {
                        LOG.warn(e.getMessage(), e);
                    }
                } else {
                    LOG.fatal("Cannot rename "+targetDir+" to "+targetDelDir);
                }
            }
            targetDir.getParentFile().mkdirs();
            if (sourceDir.renameTo(targetDir)) {
                final Loggable loggable = new RenameBinaryLoggable(db.getActiveBroker(),transaction,sourceDir,targetDir);
                try {
                    logManager.writeToLog(loggable);
                } catch (final TransactionException e) {
                    LOG.warn(e.getMessage(), e);
                }
            } else {
                LOG.fatal("Cannot move "+sourceDir+" to "+targetDir);
            }
        }
    }

    private void moveCollectionRecursive(DBBroker broker, Txn transaction, Collection collection, Collection destination, XmldbURI newName) throws PermissionDeniedException, IOException, LockException, TriggerException {
        
        final XmldbURI uri = collection.getURI();
        final CollectionCache collectionsCache = db.getCollectionsCache();
        synchronized(collectionsCache) {
                
            final XmldbURI srcURI = collection.getURI();
            final XmldbURI dstURI = destination.getURI().append(newName);

                db.getCollectionTrigger().beforeMoveCollection(broker, transaction, collection, dstURI);
        
            final XmldbURI parentName = collection.getParentURI();
            final Collection parent = openCollection(parentName, Lock.WRITE_LOCK);
            
            if(parent != null) {
                try {
                    //TODO : resolve URIs
                    parent.removeCollection(broker, uri.lastSegment());
                } finally {
                    parent.release(Lock.WRITE_LOCK);
                }
            }
            
            final CollectionStore collectionsDb = db.collectionStore();
            final Lock lock = collectionsDb.getLock();
            try {
                lock.acquire(Lock.WRITE_LOCK);
                collectionsCache.remove(collection);
                final Value key = new CollectionStore.CollectionKey(uri.toString());
                collectionsDb.remove(transaction, key);
                
                //XXX: Collection collection = new Collection(this);
                //TODO : resolve URIs destination.getURI().resolve(newName)
                collection.setPath(destination.getURI().append(newName));
                collection.setCreationTime(System.currentTimeMillis());
                destination.addCollection(broker, collection, false);
                if(parent != null) {
                    parent.saveCollection(broker, transaction);
                }
                if(parent != destination) {
                    destination.saveCollection(broker, transaction);
                }
                collection.saveCollection(broker, transaction);
            //} catch (ReadOnlyException e) {
                //throw new PermissionDeniedException(DATABASE_IS_READ_ONLY);
            } finally {
                lock.release(Lock.WRITE_LOCK);
            }
            db.getCollectionTrigger().afterMoveCollection(broker, transaction, collection, srcURI);
            
            for (XmldbURI childName : collection.childCollections()) {

                //TODO : resolve URIs !!! name.resolve(childName)
                final Collection child = openCollection(uri.append(childName), Lock.WRITE_LOCK);
                if(child == null) {
                    LOG.warn("Child collection " + childName + " not found");
                } else {
                    try {
                        moveCollectionRecursive(broker, transaction, child, collection, childName);
                    } finally {
                        child.release(Lock.WRITE_LOCK);
                    }
                }
            }
        }
    }
    
    /**
     * Removes a collection and all child collections and resources
     * 
     * We first traverse down the Collection tree to ensure that the Permissions
     * enable the Collection Tree to be removed. We then return back up the Collection
     * tree, removing each child as we progresses upwards.
     * 
     * @param txn the transaction to use
     * @param collection the collection to remove
     * @return true if the collection was removed, false otherwise
     * @throws TriggerException 
     */
    public boolean removeCollection(final Txn txn) throws PermissionDeniedException, IOException, TriggerException {
        
        if(db.isReadOnly()) {
            throw new PermissionDeniedException(Database.IS_READ_ONLY);
        }

        final Journal logManager = db.getTransactionManager().getJournal();
        
        final File fsDir = ((BrokerPool)db).fsDir();
        final File fsBackupDir = ((BrokerPool)db).fsBackupDir();
        
        final NativeBroker broker = (NativeBroker) db.getActiveBroker();
        final Subject subject = broker.getSubject();
        
        final XmldbURI parentName = getParentURI();
        final boolean isRoot = parentName == null;
        final Collection parent = isRoot ? (Collection)this : getCollection(parentName);
        
        //parent collection permissions
        if(!parent.permissions().validate(subject, Permission.WRITE)) {
            throw new PermissionDeniedException("Account '" + subject.getName() + "' is not allowed to remove collection '" + URI() + "'");
        }
        
        if(!parent.permissions().validate(subject, Permission.EXECUTE)) {
            throw new PermissionDeniedException("Account '" + subject.getName() + "' is not allowed to remove collection '" + URI() + "'");
        }
        
        //this collection permissions
        if(!permissions().validate(subject, Permission.READ)) {
            throw new PermissionDeniedException("Account '" + subject.getName() + "' is not allowed to remove collection '" + URI() + "'");
        }
        
        if(!isEmpty()) {
            if(!permissions().validate(subject, Permission.WRITE)) {
                throw new PermissionDeniedException("Account '" + subject.getName() + "' is not allowed to remove collection '" + URI() + "'");
            }

            if(!permissions().validate(subject, Permission.EXECUTE)) {
                throw new PermissionDeniedException("Account '" + subject.getName() + "' is not allowed to remove collection '" + URI() + "'");
            }
        }
        
        try {

            db.getProcessMonitor().startJob(ProcessMonitor.ACTION_REMOVE_COLLECTION, URI());
            
            db.getCollectionTrigger().beforeDeleteCollection(broker, txn, (Collection)this);
            
            final CollectionTriggersVisitor triggersVisitor = parent.getConfiguration(broker).getCollectionTriggerProxies().instantiateVisitor(broker);
            triggersVisitor.beforeDeleteCollection(broker, txn, (Collection)this);
            
            
            final long start = System.currentTimeMillis();
            final CollectionCache collectionsCache = db.getCollectionsCache();
            
            synchronized(collectionsCache) {
                final XmldbURI uri = URI();
                final String collName = uri.getRawCollectionPath();
                
                // Notify the collection configuration manager
                final CollectionConfigurationManager manager = db.getConfigurationManager();
                if(manager != null) {
                    manager.invalidate(uri);
                }
                
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Removing children collections from their parent '" + collName + "'...");
                }
                
                for(XmldbURI childName : childCollections()) {

                    //TODO : resolve from collection's base URI
                    //TODO : resulve URIs !!! (uri.resolve(childName))
                    final Collection childCollection = openCollection(uri.append(childName), Lock.WRITE_LOCK);
                    try {
                        childCollection.removeCollection(txn);
                    } finally {
                        if (childCollection != null) {
                            childCollection.getLock().release(Lock.WRITE_LOCK);
                        } else {
                            LOG.warn("childCollection is null !");
                        }
                    }
                }
                
                //Drop all index entries
                broker.notifyDropIndex((Collection)this);
                
                // Drop custom indexes
                broker.indexController.removeCollection((Collection)this, broker);
                
                if(!isRoot) {
                    // remove from parent collection
                    //TODO : resolve URIs ! (uri.resolve(".."))
                    final Collection parentCollection = openCollection(getParentURI(), Lock.WRITE_LOCK);
                    // keep the lock for the transaction
                    if(txn != null) {
                        txn.registerLock(parentCollection.getLock(), Lock.WRITE_LOCK);
                    }
                    
                    if(parentCollection != null) {
                        try {
                            LOG.debug("Removing collection '" + collName + "' from its parent...");
                            //TODO : resolve from collection's base URI
                            parentCollection.removeCollection(broker, uri.lastSegment());
                            parentCollection.saveCollection(broker, txn);
                            
                        } catch(final LockException e) {
                            LOG.warn("LockException while removing collection '" + collName + "'");
                        }
                        finally {
                            if(txn == null){
                                parentCollection.getLock().release(Lock.WRITE_LOCK);
                            }
                        }
                    }
                }
                
                final CollectionStore collectionsDb = db.collectionStore();
                
                //Update current state
                final Lock lock = collectionsDb.getLock();
                try {
                    lock.acquire(Lock.WRITE_LOCK);
                    // remove the metadata of all documents in the collection
                    final Value docKey = new CollectionStore.DocumentKey(getId());
                    final IndexQuery query = new IndexQuery(IndexQuery.TRUNC_RIGHT, docKey);
                    collectionsDb.removeAll(txn, query);
                    // if this is not the root collection remove it...
                    if(!isRoot) {
                        final Value key = new CollectionStore.CollectionKey(collName);
                        //... from the disk
                        collectionsDb.remove(txn, key);
                        //... from the cache
                        collectionsCache.remove(this);
                        //and free its id for any futher use
                        freeCollectionId(txn, getId());
                    } else {
                        //Simply save the collection on disk
                        //It will remain cached
                        //and its id well never be made available
                        saveCollection(broker, txn);
                    }
                }
                catch(final LockException e) {
                    LOG.warn("Failed to acquire lock on '" + collectionsDb.getFile().getName() + "'");
                }
                //catch(ReadOnlyException e) {
                    //throw new PermissionDeniedException(DATABASE_IS_READ_ONLY);
                //}
                catch(final BTreeException e) {
                    LOG.warn("Exception while removing collection: " + e.getMessage(), e);
                }
                catch(final IOException e) {
                    LOG.warn("Exception while removing collection: " + e.getMessage(), e);
                }
                finally {
                    lock.release(Lock.WRITE_LOCK);
                }
                
                //Remove child resources
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Removing resources in '" + collName + "'...");
                }
                
                final DOMFile domDb = db.domStore();
                
                for(final DocumentImpl doc : documents()) {
                    db.getDocumentTrigger().beforeDeleteDocument(broker, txn, doc);

                    //Remove doc's metadata
                    // WM: now removed in one step. see above.
                    //removeResourceMetadata(transaction, doc);
                    //Remove document nodes' index entries
                    new DOMTransaction(this, domDb, Lock.WRITE_LOCK) {
                        @Override
                        public Object start() {
                            try {
                                final Value ref = new NodeRef(doc.getDocId());
                                final IndexQuery query = new IndexQuery(IndexQuery.TRUNC_RIGHT, ref);
                                domDb.remove(txn, query, null);
                            } catch(final BTreeException e) {
                                LOG.warn("btree error while removing document", e);
                            } catch(final IOException e) {
                                LOG.warn("io error while removing document", e);
                            }
                            catch(final TerminatedException e) {
                                LOG.warn("method terminated", e);
                            }
                            return null;
                        }
                    }.run();
                    //Remove nodes themselves
                    new DOMTransaction(this, domDb, Lock.WRITE_LOCK) {
                        @Override
                        public Object start() {
                            if(doc.getResourceType() == DocumentImpl.BINARY_FILE) {
                                final long page = ((BinaryDocument)doc).getPage();
                                if (page > Page.NO_PAGE)
                                    {domDb.removeOverflowValue(txn, page);}
                            } else {
                                final StoredNode node = (StoredNode)doc.getFirstChild();
                                domDb.removeAll(txn, node.getInternalAddress());
                            }
                            return null;
                        }
                    }.run();
                    
                    db.getDocumentTrigger().afterDeleteDocument(broker, txn, doc.getURI());
                    
                    //Make doc's id available again
                    broker.freeResourceId(txn, doc.getDocId());
                }
                
                //now that the database has been updated, update the binary collections on disk
                final File fsSourceDir = broker.getCollectionFile(fsDir, URI(), false);
                final File fsTargetDir = broker.getCollectionFile(fsBackupDir, txn, URI(), true);

                // remove child binary collections
                if (fsSourceDir.exists()) {
                   fsTargetDir.getParentFile().mkdirs();
                   
                   //XXX: log first, rename second ??? -shabanovd
                   // DW: not sure a Fatal is required here. Copy and delete
                   // maybe?
                   if(fsSourceDir.renameTo(fsTargetDir)) {
                     final Loggable loggable = new RenameBinaryLoggable(broker, txn, fsSourceDir, fsTargetDir);
                     try {
                        logManager.writeToLog(loggable);
                     } catch (final TransactionException e) {
                         LOG.warn(e.getMessage(), e);
                     }
                   } else {
                       //XXX: throw IOException -shabanovd
                       LOG.fatal("Cannot rename "+fsSourceDir+" to "+fsTargetDir);
                   }
                }
                
                
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Removing collection '" + collName + "' took " + (System.currentTimeMillis() - start));
                }
                
                triggersVisitor.afterDeleteCollection(broker, txn, URI());
                
                db.getCollectionTrigger().afterDeleteCollection(broker, txn, URI());

                return true;
                
            }
        } finally {
            db.getProcessMonitor().endJob();
        }
    }


    /**
     * Release the collection id assigned to a collection so it can be
     * reused later.
     * 
     * @param id
     * @throws PermissionDeniedException
     */
    protected void freeCollectionId(Txn transaction, int id) throws PermissionDeniedException {
        final CollectionStore collectionsDb = db.collectionStore();
        final Lock lock = collectionsDb.getLock();
        try {
            lock.acquire(Lock.WRITE_LOCK);
            final Value key = new CollectionStore.CollectionKey(CollectionStore.FREE_COLLECTION_ID_KEY);
            final Value value = collectionsDb.get(key);
            if (value != null) {
                final byte[] data = value.getData();
                final byte[] ndata = new byte[data.length + Collection.LENGTH_COLLECTION_ID];
                System.arraycopy(data, 0, ndata, OFFSET_VALUE, data.length);
                ByteConversion.intToByte(id, ndata, OFFSET_COLLECTION_ID);
                collectionsDb.put(transaction, key, ndata, true);
            } else {
                final byte[] data = new byte[Collection.LENGTH_COLLECTION_ID];
                ByteConversion.intToByte(id, data, OFFSET_COLLECTION_ID);
                collectionsDb.put(transaction, key, data, true);
            }
        } catch (final LockException e) {
            LOG.warn("Failed to acquire lock on " + collectionsDb.getFile().getName(), e);
            //TODO : rethrow ? -pb
        //} catch (ReadOnlyException e) {
            //throw new PermissionDeniedException(DATABASE_IS_READ_ONLY);
        } finally {
            lock.release(Lock.WRITE_LOCK);
        }
    }

    /**
     * Get the next free collection id. If a collection is removed, its collection id
     * is released so it can be reused.
     * 
     * @return next free collection id.
     * @throws ReadOnlyException
     */
    public int getFreeCollectionId(Txn transaction) {
        int freeCollectionId = Collection.UNKNOWN_COLLECTION_ID;

        final CollectionStore collectionsDb = db.collectionStore();
        final Lock lock = collectionsDb.getLock();
        try {
            lock.acquire(Lock.WRITE_LOCK);
            final Value key = new CollectionStore.CollectionKey(CollectionStore.FREE_COLLECTION_ID_KEY);
            final Value value = collectionsDb.get(key);
            if (value != null) {
                final byte[] data = value.getData();
                freeCollectionId = ByteConversion.byteToInt(data, data.length - Collection.LENGTH_COLLECTION_ID);
                //LOG.debug("reusing collection id: " + freeCollectionId);
                if(data.length - Collection.LENGTH_COLLECTION_ID > 0) {
                    final byte[] ndata = new byte[data.length - Collection.LENGTH_COLLECTION_ID];
                    System.arraycopy(data, 0, ndata, OFFSET_COLLECTION_ID, ndata.length);
                    collectionsDb.put(transaction, key, ndata, true);
                } else {
                    collectionsDb.remove(transaction, key);
                }
            }
            return freeCollectionId;
        } catch (final LockException e) {
            LOG.warn("Failed to acquire lock on " + collectionsDb.getFile().getName(), e);
            return Collection.UNKNOWN_COLLECTION_ID;
            //TODO : rethrow ? -pb
        } finally {
            lock.release(Lock.WRITE_LOCK);
        }
    }

    /**
     * Get the next available unique collection id.
     * 
     * @return next available unique collection id
     * @throws ReadOnlyException
     */
    public int getNextCollectionId(Txn transaction) {
        int nextCollectionId = getFreeCollectionId(transaction);
        if (nextCollectionId != Collection.UNKNOWN_COLLECTION_ID)
            {return nextCollectionId;}

        final CollectionStore collectionsDb = db.collectionStore();
        final Lock lock = collectionsDb.getLock();
        try {
            lock.acquire(Lock.WRITE_LOCK);
            final Value key = new CollectionStore.CollectionKey(CollectionStore.NEXT_COLLECTION_ID_KEY);
            final Value data = collectionsDb.get(key);
            if (data != null) {
                nextCollectionId = ByteConversion.byteToInt(data.getData(), OFFSET_COLLECTION_ID);
                ++nextCollectionId;
            }
            final byte[] d = new byte[Collection.LENGTH_COLLECTION_ID];
            ByteConversion.intToByte(nextCollectionId, d, OFFSET_COLLECTION_ID);
            collectionsDb.put(transaction, key, d, true);
            return nextCollectionId;
        } catch (final LockException e) {
            LOG.warn("Failed to acquire lock on " + collectionsDb.getFile().getName(), e);
            return Collection.UNKNOWN_COLLECTION_ID;
            //TODO : rethrow ? -pb
        } finally {
            lock.release(Lock.WRITE_LOCK);
        }
    }
}
