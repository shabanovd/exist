/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2014 The eXist Project
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

import static org.exist.collections.CollectionConfiguration.COLLECTION_CONFIG_SUFFIX_URI;
import static org.exist.collections.CollectionConfigurationManager.CONFIG_COLLECTION_URI;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.TreeMap;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.log4j.Logger;
import org.exist.Database;
import org.exist.EXistException;
import org.exist.Indexer;
import org.exist.collections.Collection;
import org.exist.collections.CollectionConfiguration;
import org.exist.collections.CollectionConfigurationException;
import org.exist.collections.CollectionConfigurationManager;
import org.exist.collections.triggers.CollectionTrigger;
import org.exist.collections.triggers.DocumentTrigger;
import org.exist.collections.triggers.DocumentTriggersVisitor;
import org.exist.collections.triggers.TriggerException;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.dom.DocumentMetadata;
import org.exist.dom.MutableDocumentSet;
import org.exist.security.Account;
import org.exist.security.Permission;
import org.exist.security.PermissionDeniedException;
import org.exist.security.PermissionFactory;
import org.exist.security.Subject;
import org.exist.storage.btree.BTreeCallback;
import org.exist.storage.btree.BTreeException;
import org.exist.storage.btree.IndexQuery;
import org.exist.storage.btree.Value;
import org.exist.storage.cache.Cacheable;
import org.exist.storage.index.BFile;
import org.exist.storage.index.CollectionStore;
import org.exist.storage.io.VariableByteInput;
import org.exist.storage.io.VariableByteOutputStream;
import org.exist.storage.lock.Lock;
import org.exist.storage.lock.LockedDocumentMap;
import org.exist.storage.lock.MultiReadReentrantLock;
import org.exist.storage.sync.Sync;
import org.exist.storage.txn.Txn;
import org.exist.util.ByteConversion;
import org.exist.util.Configuration;
import org.exist.util.LockException;
import org.exist.util.MimeType;
import org.exist.util.ReadOnlyException;
import org.exist.util.XMLReaderObjectFactory;
import org.exist.util.XMLReaderObjectFactory.VALIDATION_SETTING;
import org.exist.util.hashtable.ObjectHashSet;
import org.exist.util.serializer.DOMStreamer;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.Constants;
import org.exist.xquery.TerminatedException;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public abstract class Stored extends Observable implements Comparable<Stored>, Cacheable {
    
    protected final static Logger LOG = Logger.getLogger(Stored.class);

    protected final static int POOL_PARSER_THRESHOLD = 500;

    private final static int SHALLOW_SIZE = 550;
    private final static int DOCUMENT_SIZE = 450;
    
    public static int OFFSET_COLLECTION_ID = 0;
    public static int OFFSET_VALUE = OFFSET_COLLECTION_ID + Collection.LENGTH_COLLECTION_ID; //2
    
    // Internal id
    private int collectionId = Collection.UNKNOWN_COLLECTION_ID;
    
    // the documents contained in this collection
    private Map<String, DocumentImpl> documents = new TreeMap<String, DocumentImpl>();
    
    // the path of this collection
    private XmldbURI path;
    
    // stores child-collections with their storage address
    private ObjectHashSet<XmldbURI> subCollections = new ObjectHashSet<XmldbURI>(19);
    
    // Storage address of the collection in the BFile
    private long address = BFile.UNKNOWN_ADDRESS;
    
    // creation time
    private long created = 0;
    
    private Observer[] observers = null;
    
    private volatile boolean collectionConfigEnabled = true;
    private boolean triggersEnabled = true;
    
    // fields required by the collections cache
    private int refCount;
    private int timestamp;
    
    private final Lock lock;
    
    /** user-defined Reader */
    protected XMLReader userReader;
    
    /** is this a temporary collection? */
    protected boolean isTempCollection;

    private Permission permissions;
    
    public Stored(XmldbURI path) {
        //The permissions assigned to this collection
        permissions = PermissionFactory.getDefaultCollectionPermission();

        setPath(path);
        lock = new MultiReadReentrantLock(path);
    }
    
    /**
     * Get the internal id.
     *
     * @return    The id value
     */
    public int getId() {
        return collectionId;
    }

    protected void setId(int id) {
        collectionId = id;
    }
    
    /**
     * Get the URI of this collection.
     */
    public XmldbURI URI() {
        return path;
    }

    /**
     * Get the name of this collection.
     *
     * @return    The name value
     */
    public XmldbURI getURI() {
        return path;
    }
    
    public final void setPath(XmldbURI uri) {
        uri = uri.toCollectionPathURI();
        
        //TODO : see if the URI resolves against DBBroker.TEMP_COLLECTION
        isTempCollection = uri.startsWith(XmldbURI.TEMP_COLLECTION_URI);
        
        path = uri;
    }
    
    /**
     * Returns the parent-collection.
     *
     * @return The parent-collection or null if this is the root collection.
     */
    public XmldbURI getParentURI() {
        if(path.equals(XmldbURI.ROOT_COLLECTION_URI)) {
            return null;
        }
        return path.removeLastSegment();
    }

    public boolean isTriggersEnabled() {
        return triggersEnabled;
    }

    public Lock getLock() {
        return lock;
    }
    
    /**
     * Set the internal storage address of the collection data.
     *
     * @param addr
     */
    public void setAddress(final long addr) {
        address = addr;
    }

    public long getAddress() {
        return address;
    }

    public void setCreationTime(final long ms) {
        created = ms;
    }

    public long getCreationTime() {
        return created;
    }

    //Cacheable methods
    @Override
    public long getKey() {
        return collectionId;
    }

    @Override
    public int getReferenceCount() {
        // TODO Auto-generated method stub
        return refCount;
    }

    @Override
    public int incReferenceCount() {
        return ++refCount;
    }

    @Override
    public int decReferenceCount() {
        return refCount > 0 ? --refCount : 0;
    }

    @Override
    public void setReferenceCount(int count) {
        refCount = count;
    }

    @Override
    public void setTimestamp(int ts) {
        timestamp = ts;
    }

    @Override
    public int getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean sync(boolean syncJournal) {
        return false;
    }

    /**
     * Check if this collection may be safely removed from the
     * cache. Returns false if there are ongoing write operations,
     * i.e. one or more of the documents is locked for write.
     *
     * @return A boolean value where true indicates it may be unloaded.
     */
    @Override
    public boolean allowUnload() {
        for(final DocumentImpl doc : documents.values()) {
            if(doc.isLockedForWrite()) {
                return false;
            }
        }
        return true;
        //try {
            //lock.acquire(Lock.WRITE_LOCK);
            //for (Iterator i = documents.values().iterator(); i.hasNext(); ) {
                //DocumentImpl doc = (DocumentImpl) i.next();
                //if (doc.isLockedForWrite())
                    //return false;
            //}
            //return true;
        //} catch (LockException e) {
            //LOG.warn("Failed to acquire lock on collection: " + getName(), e);
        //} finally {
            //lock.release();
        //}
        //return false;
    }


    @Override
    public boolean isDirty() {
        return false;
    }
    
    //Observable methods
    @Override
    public synchronized void addObserver(final Observer o) {
        if(hasObserver(o)) {
            return;
        }
        if(observers == null) {
            observers = new Observer[1];
            observers[0] = o;
        } else {
            final Observer n[] = new Observer[observers.length + 1];
            System.arraycopy(observers, 0, n, 0, observers.length);
            n[observers.length] = o;
            observers = n;
        }
    }

    private boolean hasObserver(final Observer o) {
        if(observers == null) {
            return false;
        }
        for(int i = 0; i < observers.length; i++) {
            if(observers[i] == o) {
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized void deleteObservers() {
        if(observers != null) {
            observers = null;
        }
    }

    /** add observers to the indexer
     * @param broker
     * @param indexer
     */
    protected void addObserversToIndexer(final DBBroker broker, final Indexer indexer) {
        broker.deleteObservers();
        if(observers != null) {
            for(int i = 0; i < observers.length; i++) {
                indexer.addObserver(observers[i]);
                broker.addObserver(observers[i]);
            }
        }
    }

    @Override
    public int compareTo(final Stored other) {
        if(collectionId == other.collectionId) {
            return Constants.EQUAL;
        } else if(collectionId < other.collectionId) {
            return Constants.INFERIOR;
        } else {
            return Constants.SUPERIOR;
        }
    }
    
    @Override
    public boolean equals(final Object obj) {
        if(!(obj instanceof Stored)) {
            return false;
        }
        
        return ((Stored) obj).collectionId == collectionId;
    }
    
    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append( getURI() );
        buf.append("[");
        for(final Iterator<String> i = documents.keySet().iterator(); i.hasNext(); ) {
            buf.append(i.next());
            if(i.hasNext()) {
                buf.append(", ");
            }
        }
        buf.append("]");
        return buf.toString();
    }
    
    protected final Permission permissions() {
        return permissions;
    }
    
    protected final void checkReadPerms(final Subject subject) throws PermissionDeniedException {
        if(!permissions().validate(subject, Permission.READ)) {
            throw new PermissionDeniedException("Permission denied to read collection: " + URI());
        }
    }
    
    protected final void checkWritePerms(final Subject subject) throws PermissionDeniedException {
        if(!permissions().validate(subject, Permission.WRITE)) {
            throw new PermissionDeniedException("Permission denied to write collection: " + URI());
        }
    }
    
    public CollectionConfiguration getConfiguration(final DBBroker broker) {
        if(!isCollectionConfigEnabled()) {
            return null;
        }
        
        final CollectionConfigurationManager manager = broker.getBrokerPool().getConfigurationManager();
        if(manager == null) {
            return null;
        }
        //Attempt to get configuration
        CollectionConfiguration configuration = null;
        try {
            //TODO: AR: if a Trigger throws CollectionConfigurationException
            //from its configure() method, is the rest of the collection 
            //configuration (indexes etc.) ignored even though they might be fine?
            configuration = manager.getConfiguration(broker, (Collection)this);
            setCollectionConfigEnabled(true);
        } catch(final CollectionConfigurationException e) {
            setCollectionConfigEnabled(false);
            LOG.warn("Failed to load collection configuration for '" + getURI() + "'", e);
        }
        return configuration;
    }

    /**
     * Should the collection configuration document be enabled
     * for this collection? Called by {@link org.exist.storage.NativeBroker}
     * before doing a re-index.
     *
     * @param collectionConfigEnabled
     */
    public void setCollectionConfigEnabled(final boolean collectionConfigEnabled) {
        this.collectionConfigEnabled = collectionConfigEnabled;
    }

    public boolean isCollectionConfigEnabled() {
        return collectionConfigEnabled;
    }
    
    /**
     * @param oldDoc if not null, then this document is replacing another and so WRITE access on the collection is not required,
     * just WRITE access on the old document
     */
    protected final void addDocument(final DocumentImpl doc) {
        documents.put(doc.getFileURI().getRawCollectionPath(), doc);
    }
    
    /**
     * @param oldDoc if not null, then this document is replacing another and so WRITE access on the collection is not required,
     * just WRITE access on the old document
     */
    protected final void addDocument(final Txn txn, final DBBroker broker, final DocumentImpl doc, final DocumentImpl oldDoc) throws PermissionDeniedException {
        if(oldDoc == null) {
            
            /* create */
            checkWritePerms(broker.getSubject());
        } else {
            
            /* update-replace */
            if(!oldDoc.getPermissions().validate(broker.getSubject(), Permission.WRITE)) {
                throw new PermissionDeniedException("Permission to write to overwrite document: " +  oldDoc.getURI());
            }
        }
        
        if (doc.getDocId() == DocumentImpl.UNKNOWN_DOCUMENT_ID) {
            try {
                doc.setDocId(getNextResourceId(broker, txn));
            } catch(final IOException e) {
                LOG.error("Collection error " + e.getMessage(), e);
                // TODO : re-raise the exception ? -pb
                return;
            }
        }
        addDocument(doc);
    }
    
    /**
     * Removes the document from the internal list of resources, but
     * doesn't delete the document object itself.
     *
     * @param doc
     */
    protected final void unlinkDocument(final DocumentImpl doc) throws PermissionDeniedException {
        documents.remove(doc.getFileURI().getRawCollectionPath());
    }

    /**
     * Check if the collection has a child document.
     *
     * @param  name  the name (without path) of the document
     * @return A value of true when the collection has the document identified.
     */
    protected final boolean hasDocument(final XmldbURI name) {
        return documents.containsKey(name.getRawCollectionPath());
    }
    
    /**
     * Returns the number of documents in this collection.
     *
     * @return The number of documents
     */
    public int documentsCount() {
        return documents.size();
    }
    
    protected java.util.Collection<DocumentImpl> documents() {
        return documents.values();
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
    protected final DocumentImpl document(final DBBroker broker, final XmldbURI path) throws PermissionDeniedException {
        final DocumentImpl doc = documents.get(path.getRawCollectionPath());
        if(doc != null){
            if(!doc.getPermissions().validate(broker.getSubject(), Permission.READ)) {
                throw new PermissionDeniedException("Permission denied to read document: " + path.toString());
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Document " + path + " not found!");
            }
        }
        
        return doc;
    }
    
    protected final void addDocumentsToSet(final Subject subject, final MutableDocumentSet docs, final LockedDocumentMap lockMap, final int lockType) throws LockException {
        for (final DocumentImpl doc : documents.values()) {
            if(doc.getPermissions().validate(subject, Permission.WRITE)) {
                
                doc.getUpdateLock().acquire(Lock.WRITE_LOCK);

                docs.add(doc);
                lockMap.add(doc);
            }
        }
    }
    
    protected final void addDocumentsToSet(final Subject subject, final MutableDocumentSet docs) {
        for(final DocumentImpl doc : documents.values()) {
            if(doc.getPermissions().validate(subject, Permission.READ)) {
                docs.add(doc);
            }
        }
    }

    protected final void addCollection(final Collection child) throws PermissionDeniedException {
        final XmldbURI childName = child.getURI().lastSegment();
        if(!subCollections.contains(childName)) {
            subCollections.add(childName);
        }
    }

    protected final boolean hasCollection(final XmldbURI path) {
        return subCollections.contains(path);
    }
    
    protected final Iterator<XmldbURI> collectionsStableIterator() {
        return subCollections.stableIterator();
    }

    /**
     * Remove the specified sub-collection.
     *
     * @param name
     */
    protected final void removeCollection(final XmldbURI name) throws LockException, PermissionDeniedException {
        subCollections.remove(name);
    }
    
    /**
     * Return the number of child-collections managed by this collection.
     *
     * @return The childCollectionCount value
     */
    protected final int childCollectionCount() {
        return subCollections.size();
    }
    
    protected final List<XmldbURI> childCollections() {
        return subCollections.keys();
    }
    
    protected final void checkCollectionConflict(final XmldbURI docUri) throws PermissionDeniedException {
        if(subCollections.contains(docUri.lastSegment())) {
            throw new PermissionDeniedException(
                "The collection '" + getURI() + "' already has a sub-collection named '" + docUri.lastSegment() + "',"
                + " you cannot create a Document with the same name as an existing collection."
            );
        }
    }
    
    public List<CollectionEntry> getEntries(final DBBroker broker) throws PermissionDeniedException {
        
        final List<CollectionEntry> list = new ArrayList<CollectionEntry>();
        final Iterator<XmldbURI> subCollectionIterator = subCollections.iterator();
        while(subCollectionIterator.hasNext()) {
            final XmldbURI subCollectionURI = subCollectionIterator.next();
            final CollectionEntry entry = new SubCollectionEntry(subCollectionURI);
            entry.readMetadata(broker);
            list.add(entry);
        }
        for(final DocumentImpl document : documents.values()) {
            final CollectionEntry entry = new DocumentEntry(document);
            entry.readMetadata(broker);
            list.add(entry);
        }
        return list;
    }

    public final boolean isEmpty() {
        return documents.isEmpty() && subCollections.isEmpty();
    }

    
    private void checkPermsForCollectionWrite(DBBroker broker, XmldbURI uri) throws PermissionDeniedException {
        if (broker.getDatabase().isReadOnly()) {
            throw new PermissionDeniedException(Database.IS_READ_ONLY);
        }
        
        final Subject subject = broker.getSubject();
        
        if(!permissions.validate(subject, Permission.WRITE)) {
            LOG.error("Permission denied to create collection '" + uri + "'");
            throw new PermissionDeniedException("Account '"+ subject.getName() + "' not allowed to write to collection '" + getURI() + "'");
        }
        
        if (!permissions.validate(subject, Permission.EXECUTE)) {
            LOG.error("Permission denied to create collection '" + uri + "'");
            throw new PermissionDeniedException("Account '"+ subject.getName() + "' not allowed to execute to collection '" + getURI() + "'");
        }
        
        XmldbURI name = uri.lastSegment();
        
        if (hasDocument(name)) {
            String msg = "Collection '" + getURI() + "' have document '" + name + "'";
            LOG.error(msg);
            throw new PermissionDeniedException(msg);
        }

        if (hasCollection(name)) {
            String msg = "Collection '" + getURI() + "' have collection '" + name + "'";
            LOG.error(msg);
            throw new PermissionDeniedException(msg);
        }
    }
    
    //manipulation methods
    public final Collection addCollection(DBBroker broker, Txn txn, XmldbURI uri) throws PermissionDeniedException, TriggerException, LockException, IOException {
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating collection '" + uri + "'");
        }
        
        checkPermsForCollectionWrite(broker, uri);
        
        CollectionTrigger triggersVisitor = null;
        final CollectionConfiguration conf = getConfiguration(broker);
        if(conf != null) {
            triggersVisitor = conf.getCollectionTriggerProxies().instantiateVisitor(broker);
        }

        final CollectionTrigger trigger = broker.getDatabase().getCollectionTrigger();
        trigger.beforeCreateCollection(broker, txn, uri);
        if (triggersVisitor != null) triggersVisitor.beforeCreateCollection(broker, txn, uri);

        //create collection instance
        Collection col = new Collection(broker, uri);
        
        col.setId(getNextCollectionId(broker, txn));
        col.setCreationTime(System.currentTimeMillis());
        
        //put write lock on it
        txn.acquireLock(col.getLock(), Lock.WRITE_LOCK);
        
        //add collection to set of sub collections
        subCollections.add(uri.lastSegment());

        //save both collection to disk
        saveCollection(broker, txn);

        col.saveCollection(broker, txn);
        
        trigger.afterCreateCollection(broker, txn, col);
        if (triggersVisitor != null) triggersVisitor.afterCreateCollection(broker, txn, col);
        
        return col;
    }
    
    /** 
     * Validates an XML document et prepares it for further storage. Launches prepare and postValidate triggers.
     * Since the process is dependant from the collection configuration, the collection acquires a write lock during the process.
     * 
     * @param transaction
     * @param broker
     * @param docUri
     * @param source
     * 
     * @return An {@link IndexInfo} with a write lock on the document. 
     * 
     * @throws EXistException
     * @throws PermissionDeniedException
     * @throws TriggerException
     * @throws SAXException
     * @throws LockException
     */    
    public IndexInfo validateXMLResource(final Txn transaction, final DBBroker broker, final XmldbURI docUri, final InputSource source) throws EXistException, PermissionDeniedException, TriggerException, SAXException, LockException, IOException {
        final CollectionConfiguration colconf = getConfiguration(broker);
        
        return validateXMLResourceInternal(transaction, broker, docUri, colconf, new ValidateBlock() {
            @Override
            public void run(final IndexInfo info) throws SAXException, EXistException {
                final XMLReader reader = getReader(broker, true, colconf);
                info.setReader(reader, null);
                try {
                    
                    /*
                     * Note - we must close shield the input source,
                     * else it can be closed by the Reader, so subsequently
                     * when we try and read it in storeXmlInternal we will get
                     * an exception.
                     */
                    final InputSource closeShieldedInputSource = closeShieldInputSource(source);
                    
                    reader.parse(closeShieldedInputSource);
                } catch(final SAXException e) {
                    throw new SAXException("The XML parser reported a problem: " + e.getMessage(), e);
                } catch(final IOException e) {
                    throw new EXistException(e);
                } finally {
                    releaseReader(broker, info, reader);
                }
            }
        });
    }
    
    /** 
     * Validates an XML document et prepares it for further storage. Launches prepare and postValidate triggers.
     * Since the process is dependant from the collection configuration, the collection acquires a write lock during the process.
     * 
     * @param transaction
     * @param broker
     * @param docUri
     * @param node
     * 
     * @return An {@link IndexInfo} with a write lock on the document. 
     * 
     * @throws EXistException
     * @throws PermissionDeniedException
     * @throws TriggerException
     * @throws SAXException
     * @throws LockException
     */    
    public IndexInfo validateXMLResource(final Txn transaction, final DBBroker broker, final XmldbURI docUri, final Node node) throws EXistException, PermissionDeniedException, TriggerException, SAXException, LockException, IOException {
        
        return validateXMLResourceInternal(transaction, broker, docUri, getConfiguration(broker), new ValidateBlock() {
            @Override
            public void run(final IndexInfo info) throws SAXException {
                info.setDOMStreamer(new DOMStreamer());
                info.getDOMStreamer().serialize(node, true);
            }
        });
    }

    /** 
     * Validates an XML document et prepares it for further storage. Launches prepare and postValidate triggers.
     * Since the process is dependant from the collection configuration, the collection acquires a write lock during the process.
     * 
     * @param transaction
     * @param broker
     * @param docUri
     * @param doValidate
     * 
     * @return An {@link IndexInfo} with a write lock on the document. 
     * 
     * @throws EXistException
     * @throws PermissionDeniedException
     * @throws TriggerException
     * @throws SAXException
     * @throws LockException
     */
    private IndexInfo validateXMLResourceInternal(final Txn transaction, final DBBroker broker, final XmldbURI docUri, final CollectionConfiguration config, final ValidateBlock doValidate) throws EXistException, PermissionDeniedException, TriggerException, SAXException, LockException, IOException {
        //Make the necessary operations if we process a collection configuration document
        checkConfigurationDocument(transaction, broker, docUri);
        
        final Database db = broker.getBrokerPool();
        
        if (db.isReadOnly()) {
            throw new PermissionDeniedException("Database is read-only");
        }
        
        DocumentImpl oldDoc = null;
        boolean oldDocLocked = false;
        try {
            db.getProcessMonitor().startJob(ProcessMonitor.ACTION_VALIDATE_DOC, docUri); 
            getLock().acquire(Lock.WRITE_LOCK);   
            
            DocumentImpl document = new DocumentImpl(db, (Collection)this, docUri);
            oldDoc = documents.get(docUri.getRawCollectionPath());
            checkPermissionsForAddDocument(broker, oldDoc);
            checkCollectionConflict(docUri);
            manageDocumentInformation(oldDoc, document);
            final Indexer indexer = new Indexer(broker, transaction);
            
            final IndexInfo info = new IndexInfo(indexer, config);
            info.setCreating(oldDoc == null);
            info.setOldDocPermissions(oldDoc != null ? oldDoc.getPermissions() : null);
            indexer.setDocument(document, config);
            addObserversToIndexer(broker, indexer);
            indexer.setValidating(true);
            
            if(CollectionConfiguration.DEFAULT_COLLECTION_CONFIG_FILE_URI.equals(docUri)) {
                // we are updating collection.xconf. Notify configuration manager
                //CollectionConfigurationManager confMgr = broker.getBrokerPool().getConfigurationManager();
                //confMgr.invalidateAll(getURI());
                setCollectionConfigEnabled(false);
            }
            
            final List<DocumentTrigger> triggers = new ArrayList<DocumentTrigger>();
            
            DocumentTriggersVisitor trigger = new DocumentTriggersVisitor(triggers);

            DocumentTriggersVisitor masterTriggers = (DocumentTriggersVisitor) db.getDocumentTrigger();
            //triggers.add(indexer);
            if (masterTriggers != null)
                triggers.add(masterTriggers);
            
                DocumentTriggersVisitor configTriggers = null;
            if(isTriggersEnabled() && isCollectionConfigEnabled()) {
                configTriggers = getConfiguration(broker).getDocumentTriggerProxies().instantiateVisitor(broker);
                
                if (configTriggers != null)
                        triggers.add(configTriggers);
            }
            
            trigger.setOutputHandler(indexer);
            trigger.setLexicalOutputHandler(indexer);
            trigger.setValidating(true);
            
            if(oldDoc == null) {
                trigger.beforeCreateDocument(broker, transaction, getURI().append(docUri));
            } else {
                trigger.beforeUpdateDocument(broker, transaction, oldDoc);
            }
            
            info.setTriggersVisitor(trigger);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Scanning document " + getURI().append(docUri));
            }
            doValidate.run(info);
            // new document is valid: remove old document
            if (oldDoc != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("removing old document " + oldDoc.getFileURI());
                }
                oldDoc.getUpdateLock().acquire(Lock.WRITE_LOCK);
                oldDocLocked = true;
                if (oldDoc.getResourceType() == DocumentImpl.BINARY_FILE) {
                    //TODO : use a more elaborated method ? No triggers...
                    broker.removeBinaryResource(transaction, (BinaryDocument) oldDoc);
                    documents.remove(oldDoc.getFileURI().getRawCollectionPath());
                    //This lock is released in storeXMLInternal()
                    //TODO : check that we go until there to ensure the lock is released
//                    if (transaction != null)
//                      transaction.acquireLock(document.getUpdateLock(), Lock.WRITE_LOCK);
//                      else
                    document.getUpdateLock().acquire(Lock.WRITE_LOCK);
                    
                    document.setDocId(getNextResourceId(broker, transaction));
                    addDocument(document);
                } else {
                    //TODO : use a more elaborated method ? No triggers...
                    broker.removeXMLResource(transaction, oldDoc, false);
                    oldDoc.copyOf(document);
                    indexer.setDocumentObject(oldDoc);
                    //old has become new at this point
                    document = oldDoc;
                    oldDocLocked = false;               
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("removed old document " + oldDoc.getFileURI());
                }
            } else {
                //This lock is released in storeXMLInternal()
                //TODO : check that we go until there to ensure the lock is released
//              if (transaction != null)
//                      transaction.acquireLock(document.getUpdateLock(), Lock.WRITE_LOCK);
//              else
                document.getLock().acquire(Lock.WRITE_LOCK);
                
                document.setDocId(getNextResourceId(broker, transaction));
                addDocument(document);
            }
            
            indexer.setValidating(false);
            trigger.setValidating(false);
//              masterTriggers.setValidating(false);
//            if(configTriggers != null) {
//              configTriggers.setValidating(false);
//            }
            return info;
        } finally {
            if (oldDoc != null && oldDocLocked) {
                oldDoc.getUpdateLock().release(Lock.WRITE_LOCK);
            }
            getLock().release(Lock.WRITE_LOCK);
            
            db.getProcessMonitor().endJob();
        }
    }
    
    //stops streams on the input source from being closed
    private InputSource closeShieldInputSource(final InputSource source) {
        
        final InputSource protectedInputSource = new InputSource();
        protectedInputSource.setEncoding(source.getEncoding());
        protectedInputSource.setSystemId(source.getSystemId());
        protectedInputSource.setPublicId(source.getPublicId());
        
        if(source.getByteStream() != null) {
            //TODO consider AutoCloseInputStream
            final InputStream closeShieldByteStream = new CloseShieldInputStream(source.getByteStream());
            protectedInputSource.setByteStream(closeShieldByteStream);
        }
        
        if(source.getCharacterStream() != null) {
            //TODO consider AutoCloseReader
            final Reader closeShieldReader = new CloseShieldReader(source.getCharacterStream());
            protectedInputSource.setCharacterStream(closeShieldReader);
        }
        
        return protectedInputSource;
    }
    
    private class CloseShieldReader extends Reader {
        private final Reader reader;
        public CloseShieldReader(final Reader reader) {
            this.reader = reader;
        }

        @Override
        public int read(final char[] cbuf, final int off, final int len) throws IOException {
            return reader.read(cbuf, off, len);
        }

        @Override
        public void close() throws IOException {
            //do nothing as we are close shield
        }
    }

    private void checkConfigurationDocument(final Txn transaction, final DBBroker broker, final XmldbURI docUri) throws EXistException, PermissionDeniedException, LockException {
        //Is it a collection configuration file ?
        //TODO : use XmldbURI.resolve() !
        if (!getURI().startsWith(CONFIG_COLLECTION_URI)) {
            return;
        }
        if(!docUri.endsWith(COLLECTION_CONFIG_SUFFIX_URI)) {
            return;
        }
        //Allow just one configuration document per collection
        //TODO : do not throw the exception if a system property allows several ones -pb
        for(DocumentImpl confDoc : documents.values()) {
            final XmldbURI currentConfDocName = confDoc.getFileURI();
            if(currentConfDocName != null && !currentConfDocName.equals(docUri)) {
                throw new EXistException("Could not store configuration '" + docUri + "': A configuration document with a different name ("
                    + currentConfDocName + ") already exists in this collection (" + getURI() + ")");
            }
        }
        //broker.saveCollection(transaction, this);
        //CollectionConfigurationManager confMgr = broker.getBrokerPool().getConfigurationManager();
        //if(confMgr != null)
            //try {
                //confMgr.reload(broker, this);
            // catch (CollectionConfigurationException e) {
                //throw new EXistException("An error occurred while reloading the updated collection configuration: " + e.getMessage(), e);
        //}
    }
    
    /**
     * Check Permissions about user and document when a document is added to the databse, and throw exceptions if necessary.
     *
     * @param broker
     * @param oldDoc old Document existing in database prior to adding a new one with same name, or null if none exists
     * @throws LockException
     * @throws PermissionDeniedException
     */
    private void checkPermissionsForAddDocument(final DBBroker broker, final DocumentImpl oldDoc) throws LockException, PermissionDeniedException {
        
        // do we have execute permission on the collection?
        if(!permissions().validate(broker.getSubject(), Permission.EXECUTE)) {
            throw new PermissionDeniedException("Execute permission is not granted on the Collection.");
        }
            
        if(oldDoc != null) {   
            
            /* update document */
            
            LOG.debug("Found old doc " + oldDoc.getDocId());
            
            // check if the document is locked by another user
            final Account lockUser = oldDoc.getUserLock();
            if(lockUser != null && !lockUser.equals(broker.getSubject())) {
                throw new PermissionDeniedException("The document is locked by user '" + lockUser.getName() + "'.");
            }
            
            // do we have write permission on the old document or are we the owner of the old document?
            if (!((oldDoc.getPermissions().getOwner().getId() == broker.getSubject().getId()) || (oldDoc.getPermissions().validate(broker.getSubject(), Permission.WRITE)))) {
                throw new PermissionDeniedException("A resource with the same name already exists in the target collection '" + URI() + "', and you do not have write access on that resource.");
            }
        } else {
            
            /* create document */
            checkWritePerms(broker.getSubject());
        }
    }
    
    /** If an old document exists, keep information  about  the document.
     * @param broker
     * @param document
     */
    private void manageDocumentInformation(final DocumentImpl oldDoc, final DocumentImpl document) {
        DocumentMetadata metadata = new DocumentMetadata();
        if (oldDoc != null) {
            metadata = oldDoc.getMetadata();
            metadata.setCreated(oldDoc.getMetadata().getCreated());
            metadata.setLastModified(System.currentTimeMillis());
            document.setPermissions(oldDoc.getPermissions());
        } else {
                //Account user = broker.getSubject();
                metadata.setCreated(System.currentTimeMillis());

                /*
            if(!document.getPermissions().getOwner().equals(user)) {
                document.getPermissions().setOwner(broker.getSubject(), user);
            }

            CollectionConfiguration config = getConfiguration(broker);
            if (config != null) {
                document.getPermissions().setMode(config.getDefResPermissions());
                group = config.getDefResGroup(user);
            } else {
                group = user.getPrimaryGroup();
            }

            if(!document.getPermissions().getGroup().equals(group)) {
                document.getPermissions().setGroup(broker.getSubject(), group);
            }*/
        }
        document.setMetadata(metadata);
    }

    // Streaming
    public BinaryDocument addBinaryResource(final Txn transaction, final DBBroker broker, final BinaryDocument blob, final InputStream is, final String mimeType, final long size, final Date created, final Date modified) throws EXistException, PermissionDeniedException, LockException, TriggerException, IOException {
        final Database db = broker.getBrokerPool();
        if (db.isReadOnly()) {
            throw new PermissionDeniedException("Database is read-only");
        }
        final XmldbURI docUri = blob.getFileURI();
        //TODO : move later, i.e. after the collection lock is acquired ?
        final DocumentImpl oldDoc = document(broker, docUri);
        try {
            db.getProcessMonitor().startJob(ProcessMonitor.ACTION_STORE_BINARY, docUri);
            getLock().acquire(Lock.WRITE_LOCK);
            checkPermissionsForAddDocument(broker, oldDoc);
            checkCollectionConflict(docUri);
            manageDocumentInformation(oldDoc, blob);
            final DocumentMetadata metadata = blob.getMetadata();
            metadata.setMimeType(mimeType == null ? MimeType.BINARY_TYPE.getName() : mimeType);
            if (created != null) {
                metadata.setCreated(created.getTime());
            }
            if (modified != null) {
                metadata.setLastModified(modified.getTime());
            }
            blob.setContentLength(size);
            if (oldDoc == null) {
                db.getDocumentTrigger().beforeCreateDocument(broker, transaction, blob.getURI());
            } else {
                db.getDocumentTrigger().beforeUpdateDocument(broker, transaction, oldDoc);
            }
            DocumentTriggersVisitor triggersVisitor = null;
            if (isTriggersEnabled()) {
                triggersVisitor = getConfiguration(broker).getDocumentTriggerProxies().instantiateVisitor(broker);
                if (oldDoc == null) {
                    triggersVisitor.beforeCreateDocument(broker, transaction, blob.getURI());
                } else {
                    triggersVisitor.beforeUpdateDocument(broker, transaction, oldDoc);
                }
            }
            if (oldDoc != null) {
                LOG.debug("removing old document " + oldDoc.getFileURI());
                if (oldDoc instanceof BinaryDocument) {
                    broker.removeBinaryResource(transaction, (BinaryDocument) oldDoc);
                } else {
                    broker.removeXMLResource(transaction, oldDoc);
                }
            }
            broker.storeBinaryResource(transaction, blob, is);
            addDocument(transaction, broker, blob, oldDoc);
            broker.storeXMLResource(transaction, blob);
            if (oldDoc == null) {
                db.getDocumentTrigger().afterCreateDocument(broker, transaction, blob);
            } else {
                db.getDocumentTrigger().afterUpdateDocument(broker, transaction, blob);
            }
            if (isTriggersEnabled()) {
                //Strange ! What is the "if" clause for ? -pb
                if (oldDoc == null) {
                    triggersVisitor.afterCreateDocument(broker, transaction, blob);
                } else {
                    triggersVisitor.afterUpdateDocument(broker, transaction, blob);
                }
            }
            return blob;
        } finally {
            broker.getBrokerPool().getProcessMonitor().endJob();
            getLock().release(Lock.WRITE_LOCK);
        }
    }
    
    /**
     * Remove the specified document from the collection.
     *
     * @param  txn
     * @param  broker
     * @param  docUri
     */
    public void removeXMLResource(final Txn txn, final DBBroker broker, final XmldbURI docUri) throws PermissionDeniedException, TriggerException, LockException {
        
        checkWritePerms(broker.getSubject());
        
        DocumentImpl doc = null;
        
        final BrokerPool db = broker.getBrokerPool();
        try {
            db.getProcessMonitor().startJob(ProcessMonitor.ACTION_REMOVE_XML, docUri);

            getLock().acquire(Lock.WRITE_LOCK);
            
            doc = documents.get(docUri.getRawCollectionPath());
            
            if (doc == null) {
                return; //TODO should throw an exception!!! Otherwise we dont know if the document was removed
            }
            
            doc.getUpdateLock().acquire(Lock.WRITE_LOCK);
            
            boolean useTriggers = isTriggersEnabled();
            if (CollectionConfiguration.DEFAULT_COLLECTION_CONFIG_FILE_URI.equals(docUri)) {
                // we remove a collection.xconf configuration file: tell the configuration manager to
                // reload the configuration.
                useTriggers = false;
                final CollectionConfigurationManager confMgr = broker.getBrokerPool().getConfigurationManager();
                if (confMgr != null) {
                    confMgr.invalidate(getURI());
                }
            }
            
            DocumentTriggersVisitor triggersVisitor = null;
            if(useTriggers) {
                triggersVisitor = getConfiguration(broker).getDocumentTriggerProxies().instantiateVisitor(broker);
                triggersVisitor.beforeDeleteDocument(broker, txn, doc);
            }
            
            broker.removeXMLResource(txn, doc);
            documents.remove(docUri.getRawCollectionPath());
            
            if(useTriggers) {
                triggersVisitor.afterDeleteDocument(broker, txn, getURI().append(docUri));
            }
            
            broker.getBrokerPool().getNotificationService().notifyUpdate(doc, UpdateListener.REMOVE);
        } finally {
            broker.getBrokerPool().getProcessMonitor().endJob();
            if(doc != null) {
                doc.getUpdateLock().release(Lock.WRITE_LOCK);
            }
            getLock().release(Lock.WRITE_LOCK);
        }
    }
    
    public void removeBinaryResource(final Txn transaction, final DBBroker broker, final DocumentImpl doc) throws PermissionDeniedException, LockException, TriggerException {
        checkWritePerms(broker.getSubject());
        
        if(doc == null) {
            return;  //TODO should throw an exception!!! Otherwise we dont know if the document was removed
        }
        
        try {
            broker.getBrokerPool().getProcessMonitor().startJob(ProcessMonitor.ACTION_REMOVE_BINARY, doc.getFileURI());
            getLock().acquire(Lock.WRITE_LOCK);
            
            if(doc.getResourceType() != DocumentImpl.BINARY_FILE) {
                throw new PermissionDeniedException("document " + doc.getFileURI() + " is not a binary object");
            }
            
            if(doc.isLockedForWrite()) {
                throw new PermissionDeniedException("Document " + doc.getFileURI() + " is locked for write");
            }
            
            doc.getUpdateLock().acquire(Lock.WRITE_LOCK);

            DocumentTriggersVisitor triggersVisitor = null;
            if(isTriggersEnabled()) {
                triggersVisitor = getConfiguration(broker).getDocumentTriggerProxies().instantiateVisitor(broker);
                triggersVisitor.beforeDeleteDocument(broker, transaction, doc);
            }
            

            try {
               broker.removeBinaryResource(transaction, (BinaryDocument) doc);
            } catch (final IOException ex) {
               throw new PermissionDeniedException("Cannot delete file: " + doc.getURI().toString() + ": " + ex.getMessage(), ex);
            }
            
            documents.remove(doc.getFileURI().getRawCollectionPath());
            
            if(isTriggersEnabled()) {
                triggersVisitor.afterDeleteDocument(broker, transaction, doc.getURI());
            }

        } finally {
            broker.getBrokerPool().getProcessMonitor().endJob();
            doc.getUpdateLock().release(Lock.WRITE_LOCK);
            getLock().release(Lock.WRITE_LOCK);
        }
    }

    /** Stores an XML document in the database. {@link #validateXMLResourceInternal(org.exist.storage.txn.Txn,
     * org.exist.storage.DBBroker, org.exist.xmldb.XmldbURI, CollectionConfiguration, org.exist.collections.Collection.ValidateBlock)} 
     * should have been called previously in order to acquire a write lock for the document. Launches the finish trigger.
     * 
     * @param transaction
     * @param broker
     * @param info
     * @param source
     * @param privileged
     * 
     * @throws EXistException
     * @throws PermissionDeniedException
     * @throws TriggerException
     * @throws SAXException
     * @throws LockException
     */
    public void store(final Txn transaction, final DBBroker broker, final org.exist.collections.IndexInfo info, final InputSource source, boolean privileged) throws EXistException, PermissionDeniedException, TriggerException, SAXException, LockException {
        
        storeXMLInternal(transaction, broker, info, privileged, new StoreBlock() {
            @Override
            public void run() throws EXistException, SAXException {
                try {
                    final InputStream is = source.getByteStream();
                    if(is != null && is.markSupported()) {
                        is.reset();
                    } else {
                        final Reader cs = source.getCharacterStream();
                        if(cs != null && cs.markSupported()) {
                            cs.reset();
                        }
                    }
                } catch(final IOException e) {
                    // mark is not supported: exception is expected, do nothing
                    LOG.debug("InputStream or CharacterStream underlying the InputSource does not support marking and therefore cannot be re-read.");
                }
                final XMLReader reader = getReader(broker, false, info.getCollectionConfig());
                ((IndexInfo)info).setReader(reader, null);
                try {
                    reader.parse(source);
                } catch(final IOException e) {
                    throw new EXistException(e);
                } finally {
                    releaseReader(broker, info, reader);
                }
            }
        });
    }
    
    /** 
     * Stores an XML document in the database. {@link #validateXMLResourceInternal(org.exist.storage.txn.Txn,
     * org.exist.storage.DBBroker, org.exist.xmldb.XmldbURI, CollectionConfiguration, org.exist.collections.Collection.ValidateBlock)} 
     * should have been called previously in order to acquire a write lock for the document. Launches the finish trigger.
     * 
     * @param transaction
     * @param broker
     * @param info
     * @param data
     * @param privileged
     * 
     * @throws EXistException
     * @throws PermissionDeniedException
     * @throws TriggerException
     * @throws SAXException
     * @throws LockException
     */
    public void store(final Txn transaction, final DBBroker broker, final org.exist.collections.IndexInfo info, final String data, boolean privileged) throws EXistException, PermissionDeniedException, TriggerException, SAXException, LockException {
        
        storeXMLInternal(transaction, broker, info, privileged, new StoreBlock() {
            @Override
            public void run() throws SAXException, EXistException {
                final CollectionConfiguration colconf = info.getDocument().getCollection().getConfiguration(broker);
                final XMLReader reader = getReader(broker, false, colconf);
                ((IndexInfo)info).setReader(reader, null);
                try {
                    reader.parse(new InputSource(new StringReader(data)));
                } catch(final IOException e) {
                    throw new EXistException(e);
                } finally {
                    releaseReader(broker, info, reader);
                }
            }
        });
    }

    /** 
     * Stores an XML document in the database. {@link #validateXMLResourceInternal(org.exist.storage.txn.Txn,
     * org.exist.storage.DBBroker, org.exist.xmldb.XmldbURI, CollectionConfiguration, org.exist.collections.Collection.ValidateBlock)} 
     * should have been called previously in order to acquire a write lock for the document. Launches the finish trigger.
     * 
     * @param transaction
     * @param broker
     * @param info
     * @param node
     * @param privileged
     * 
     * @throws EXistException
     * @throws PermissionDeniedException
     * @throws TriggerException
     * @throws SAXException
     * @throws LockException
     */  
    public void store(final Txn transaction, final DBBroker broker, final org.exist.collections.IndexInfo info, final Node node, boolean privileged) throws EXistException, PermissionDeniedException, TriggerException, SAXException, LockException {
        
        checkWritePerms(broker.getSubject());
        
        storeXMLInternal(transaction, broker, info, privileged, new StoreBlock() {
            @Override
            public void run() throws EXistException, SAXException {
                info.getDOMStreamer().serialize(node, true);
            }
        });
    }

    private interface StoreBlock {
        public void run() throws EXistException, SAXException;
    }

    /** 
     * Stores an XML document in the database. {@link #validateXMLResourceInternal(org.exist.storage.txn.Txn,
     * org.exist.storage.DBBroker, org.exist.xmldb.XmldbURI, CollectionConfiguration, org.exist.collections.Collection.ValidateBlock)} 
     * should have been called previously in order to acquire a write lock for the document. Launches the finish trigger.
     * 
     * @param txn
     * @param broker
     * @param info
     * @param privileged
     * @param doParse
     * 
     * @throws EXistException
     * @throws SAXException
     */
    private void storeXMLInternal(final Txn txn, final DBBroker broker, final org.exist.collections.IndexInfo info, final boolean privileged, final StoreBlock doParse) throws EXistException, SAXException, PermissionDeniedException {
        
        final DocumentImpl document = info.getIndexer().getDocument();
        
        final Database db = broker.getBrokerPool();
        
        try {
            /* TODO
             * 
             * These security checks are temporarily disabled because throwing an exception
             * here may cause the database to corrupt.
             * Why would the database corrupt? Because validateXMLInternal that is typically
             * called before this method actually modifies the database and this collection,
             * so throwing an exception here leaves the database in an inconsistent state
             * with data 1/2 added/updated.
             * 
             * The downside of disabling these checks here is that: this collection is not locked
             * between the call to validateXmlInternal and storeXMLInternal, which means that if
             * UserA in ThreadA calls validateXmlInternal and is permitted access to store a resource,
             * and then UserB in ThreadB modifies the permissions of this collection to prevent UserA
             * from storing the document, when UserA reaches here (storeXMLInternal) they will still
             * be allowed to store their document. However the next document that UserA attempts to store
             * will be forbidden by validateXmlInternal and so the security transgression whilst not ideal
             * is short-lived.
             * 
             * To fix the issue we need to refactor validateXMLInternal and move any document/database/collection
             * modification code into storeXMLInternal after the commented out permissions checks below.
             * 
             * Noted by Adam Retter 2012-02-01T19:18
             */
            
            /*
            if(info.isCreating()) {
                // create
                * 
                if(!getPermissionsNoLock().validate(broker.getSubject(), Permission.WRITE)) {
                    throw new PermissionDeniedException("Permission denied to write collection: " + path);
                }
            } else {
                // update

                final Permission oldDocPermissions = info.getOldDocPermissions();
                if(!((oldDocPermissions.getOwner().getId() != broker.getSubject().getId()) | (oldDocPermissions.validate(broker.getSubject(), Permission.WRITE)))) {
                    throw new PermissionDeniedException("A resource with the same name already exists in the target collection '" + path + "', and you do not have write access on that resource.");
                }
            }
            */

            if(LOG.isDebugEnabled()) {
                LOG.debug("storing document " + document.getDocId() + " ...");
            }

            //Sanity check
            if(!document.getUpdateLock().isLockedForWrite()) {
                LOG.warn("document is not locked for write !");
            }
            
            db.getProcessMonitor().startJob(ProcessMonitor.ACTION_STORE_DOC, document.getFileURI());
            doParse.run();
            broker.storeXMLResource(txn, document);
            broker.flush();
            broker.closeDocument();
            //broker.checkTree(document);
            LOG.debug("document stored.");
        } finally {
            //This lock has been acquired in validateXMLResourceInternal()
            document.getUpdateLock().release(Lock.WRITE_LOCK);
            broker.getBrokerPool().getProcessMonitor().endJob();
        }
        setCollectionConfigEnabled(true);
        broker.deleteObservers();
        
//        if(info.isCreating()) {
//            db.getDocumentTrigger().afterCreateDocument(broker, transaction, document);
//        } else {
//            db.getDocumentTrigger().afterUpdateDocument(broker, transaction, document);
//        }
//        
//        if(isTriggersEnabled() && isCollectionConfigEnabled() && info.getTriggersVisitor() != null) {
            if(info.isCreating()) {
                info.getTriggersVisitor().afterCreateDocument(broker, txn, document);
            } else {
                info.getTriggersVisitor().afterUpdateDocument(broker, txn, document);
            }
//        }
        
        db.getNotificationService().notifyUpdate(document, (info.isCreating() ? UpdateListener.ADD : UpdateListener.UPDATE));
        //Is it a collection configuration file ?
        final XmldbURI docName = document.getFileURI();
        //WARNING : there is no reason to lock the collection since setPath() is normally called in a safe way
        //TODO: *resolve* URI against CollectionConfigurationManager.CONFIG_COLLECTION_URI 
        if (getURI().startsWith(CONFIG_COLLECTION_URI)
                && docName.endsWith(COLLECTION_CONFIG_SUFFIX_URI)) {
            broker.sync(Sync.MAJOR_SYNC);
            final CollectionConfigurationManager manager = broker.getBrokerPool().getConfigurationManager();
            if(manager != null) {
                try {
                    manager.invalidate(getURI());
                    manager.loadConfiguration(broker, (Collection)this);
                } catch(final PermissionDeniedException pde) {
                    throw new EXistException(pde.getMessage(), pde);
                } catch(final LockException le) {
                    throw new EXistException(le.getMessage(), le);
                } catch(final CollectionConfigurationException e) { 
                    // DIZ: should this exception really been thrown? bugid=1807744
                    throw new EXistException("Error while reading new collection configuration: " + e.getMessage(), e);
                }
            }
        }
    }

    /** 
     * Validates an XML document and prepares it for further storage.
     * Launches prepare and postValidate triggers.
     * Since the process is dependent from the collection configuration, 
     * the collection acquires a write lock during the process.
     * 
     * @param transaction
     * @param broker
     * @param docUri   
     * @param data  
     * 
     * @return An {@link IndexInfo} with a write lock on the document. 
     * 
     * @throws EXistException
     * @throws PermissionDeniedException
     * @throws TriggerException
     * @throws SAXException
     * @throws LockException
     */    
    public IndexInfo validateXMLResource(final Txn transaction, final DBBroker broker, final XmldbURI docUri, final String data) throws EXistException, PermissionDeniedException, TriggerException, SAXException, LockException, IOException {
        return validateXMLResource(transaction, broker, docUri, new InputSource(new StringReader(data)));
    }

    //IO operations
    /**
     * Saves the specified collection to storage. Collections are usually cached in
     * memory. If a collection is modified, this method needs to be called to make
     * the changes persistent.
     * 
     * Note: appending a new document to a collection does not require a save.
     * 
     * @throws PermissionDeniedException 
     * @throws IOException 
     * @throws TriggerException 
     */
    protected void saveCollection(DBBroker broker, Txn transaction) throws PermissionDeniedException, IOException, TriggerException {
        if (broker.getDatabase().isReadOnly()) {
            throw new PermissionDeniedException(Database.IS_READ_ONLY);
        }
        
        broker.getDatabase().getCollectionsCache().add((Collection)this);
        
        final Lock lock = broker.collectionsDb.getLock();
        try {
            lock.acquire(Lock.WRITE_LOCK);
            
            if(getId() == Collection.UNKNOWN_COLLECTION_ID) {
                setId(getNextCollectionId(broker, transaction));
            }
            
            final Value name = new CollectionStore.CollectionKey(getURI().toString());
            final VariableByteOutputStream ostream = new VariableByteOutputStream(8);
            
            write(ostream);
            
            final long addr = broker.collectionsDb.put(transaction, name, ostream.data(), true);
            if (addr == BFile.UNKNOWN_ADDRESS) {
                //TODO : exception !!! -pb
                LOG.warn("could not store collection data for '" + getURI()+ "'");
                return;
            }
            setAddress(addr);
            ostream.close();
            
        } catch (final LockException e) {
            LOG.warn("Failed to acquire lock on " + broker.collectionsDb.getFile().getName(), e);
        } finally {
            lock.release(Lock.WRITE_LOCK);
        }
    }
    
    /**
     * Write collection contents to stream.
     *
     * @param ostream
     * @throws IOException
     */
    private void write(final VariableByteOutputStream ostream) throws IOException {
        ostream.writeInt(collectionId);
        ostream.writeInt(subCollections.size());
        
        for(final Iterator<XmldbURI> i = subCollections.iterator(); i.hasNext(); ) {
            final XmldbURI childCollectionURI = i.next();
            ostream.writeUTF(childCollectionURI.toString());
        }
        permissions.write(ostream);
        ostream.writeLong(created);
    }
    
    /**
     * Read collection contents from the stream.
     *
     * @param istream
     * @throws IOException
     * @throws LockException 
     */
    //XXX: protected
    public void read(final DBBroker broker, final VariableByteInput istream) throws IOException, PermissionDeniedException, LockException {
        collectionId = istream.readInt();
        
        final int collLen = istream.readInt();
        subCollections = new ObjectHashSet<XmldbURI>(collLen == 0 ? 19 : collLen); //TODO what is this number 19?
        for (int i = 0; i < collLen; i++) {
            subCollections.add(XmldbURI.create(istream.readUTF()));
        }
        
        permissions.read(istream);

        created = istream.readLong();
        
        if(!permissions.validate(broker.getSubject(), Permission.EXECUTE)) {
            throw new PermissionDeniedException("Permission denied to open the Collection " + path);
        }
        
        final Stored col = this;
        final Lock lock = broker.collectionsDb.getLock();
        lock.acquire(Lock.READ_LOCK);
        try {
            Value key = new CollectionStore.DocumentKey(collectionId);
            IndexQuery query = new IndexQuery(IndexQuery.TRUNC_RIGHT, key);
            
            broker.collectionsDb.query(query, new BTreeCallback() {

                @Override
                public boolean indexInfo(Value key, long pointer) throws TerminatedException {
                    try {
                        final byte type = key.data()[key.start() + Collection.LENGTH_COLLECTION_ID + DocumentImpl.LENGTH_DOCUMENT_TYPE]; 
                        final VariableByteInput istream = broker.collectionsDb.getAsStream(pointer);
                        
                        final DocumentImpl doc;
                        if (type == DocumentImpl.BINARY_FILE) {
                            doc = new BinaryDocument(broker.getDatabase());
                        } else {
                            doc = new DocumentImpl(broker.getDatabase());
                        }
                        doc.read(istream);

                        //
                        doc.setCollection((Collection)col);
                        
                        if(doc.getDocId() == DocumentImpl.UNKNOWN_DOCUMENT_ID) {
                            LOG.error("Document must have ID. ["+doc+"]");
                            throw new EXistException("Document must have ID.");
                        }
                        
                        documents.put(doc.getFileURI().getRawCollectionPath(), doc);

                    } catch (final EOFException e) {
                        LOG.error("EOFException while reading document data", e);
                    } catch (final IOException e) {
                        LOG.error("IOException while reading document data", e);
                    } catch(final EXistException ee) {
                        LOG.error("EXistException while reading document data", ee);
                    }
                    
                    return true;
                }
            });

        } catch (final IOException e) {
            LOG.warn("IOException while reading document data", e);
        } catch (final BTreeException e) {
            LOG.warn("Exception while reading document data", e);
        } catch (final TerminatedException e) {
            LOG.warn("Exception while reading document data", e);
        } finally {
            lock.release(Lock.READ_LOCK);
        }
    }
    
    /**
     * Get the next available unique collection id.
     * 
     * @return next available unique collection id
     * @throws LockException 
     * @throws ReadOnlyException
     */
    protected static int getNextCollectionId(final DBBroker broker, Txn transaction) throws LockException {
        int nextCollectionId = getFreeCollectionId(broker, transaction);
        
        if (nextCollectionId != Collection.UNKNOWN_COLLECTION_ID) {
            return nextCollectionId;
        }
        
        final Lock lock = broker.collectionsDb.getLock();
        
        lock.acquire(Lock.WRITE_LOCK);
        try {
            final Value key = new CollectionStore.CollectionKey(CollectionStore.NEXT_COLLECTION_ID_KEY);
            final Value data = broker.collectionsDb.get(key);
            if (data != null) {
                nextCollectionId = ByteConversion.byteToInt(data.getData(), OFFSET_COLLECTION_ID);
                ++nextCollectionId;
            }
            final byte[] d = new byte[Collection.LENGTH_COLLECTION_ID];
            ByteConversion.intToByte(nextCollectionId, d, OFFSET_COLLECTION_ID);
            broker.collectionsDb.put(transaction, key, d, true);
            return nextCollectionId;

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
    private static int getFreeCollectionId(final DBBroker broker, Txn transaction) {
        int freeCollectionId = Collection.UNKNOWN_COLLECTION_ID;
        final Lock lock = broker.collectionsDb.getLock();
        try {
            lock.acquire(Lock.WRITE_LOCK);
            try {
                final Value key = new CollectionStore.CollectionKey(CollectionStore.FREE_COLLECTION_ID_KEY);
                final Value value = broker.collectionsDb.get(key);
                if (value != null) {
                    final byte[] data = value.getData();
                    freeCollectionId = ByteConversion.byteToInt(data, data.length - Collection.LENGTH_COLLECTION_ID);
                    //LOG.debug("reusing collection id: " + freeCollectionId);
                    if(data.length - Collection.LENGTH_COLLECTION_ID > 0) {
                        final byte[] ndata = new byte[data.length - Collection.LENGTH_COLLECTION_ID];
                        System.arraycopy(data, 0, ndata, OFFSET_COLLECTION_ID, ndata.length);
                        broker.collectionsDb.put(transaction, key, ndata, true);
                    } else {
                        broker.collectionsDb.remove(transaction, key);
                    }
                }
                return freeCollectionId;
            } finally {
                lock.release(Lock.WRITE_LOCK);
            }
        } catch (final LockException e) {
            LOG.warn("Failed to acquire lock on " + broker.collectionsDb.getFile().getName(), e);
            return Collection.UNKNOWN_COLLECTION_ID;
        }
    }
    
    /**
     * Get the next unused document id. If a document is removed, its doc id is
     * released, so it can be reused.
     * 
     * @return Next unused document id
     * @throws ReadOnlyException
     */
    private int getFreeResourceId(DBBroker broker, Txn txn) throws ReadOnlyException {
        int freeDocId = DocumentImpl.UNKNOWN_DOCUMENT_ID;
        final Lock lock = broker.collectionsDb.getLock();
        try {
            lock.acquire(Lock.WRITE_LOCK);
            final Value key = new CollectionStore.CollectionKey(CollectionStore.FREE_DOC_ID_KEY);
            final Value value = broker.collectionsDb.get(key);
            if (value != null) {
                final byte[] data = value.getData();
                freeDocId = ByteConversion.byteToInt(data, data.length - 4);
                //LOG.debug("reusing document id: " + freeDocId);
                if(data.length - 4 > 0) {
                    final byte[] ndata = new byte[data.length - 4];
                    System.arraycopy(data, 0, ndata, 0, ndata.length);
                    broker.collectionsDb.put(txn, key, ndata, true);
                } else {
                    broker.collectionsDb.remove(txn, key);
                }
            }
            //TODO : maybe something ? -pb
        } catch (final LockException e) {
            LOG.warn("Failed to acquire lock on " + broker.collectionsDb.getFile().getName(), e);
            return DocumentImpl.UNKNOWN_DOCUMENT_ID;
            //TODO : rethrow ? -pb
        } finally {
            lock.release(Lock.WRITE_LOCK);
        }
        return freeDocId;
    }

    /** get next Free Doc Id 
     * @throws EXistException If there's no free document id */
    protected int getNextResourceId(DBBroker broker, Txn txn) throws IOException {
        int nextDocId;
        try {
            nextDocId = getFreeResourceId(broker, txn);
        } catch (final ReadOnlyException e) {
            //TODO : rethrow ? -pb
            return 1;
        }
        if (nextDocId != DocumentImpl.UNKNOWN_DOCUMENT_ID)
            {return nextDocId;}
        nextDocId = 1;
        final Lock lock = broker.collectionsDb.getLock();
        try {
            lock.acquire(Lock.WRITE_LOCK);
            final Value key = new CollectionStore.CollectionKey(CollectionStore.NEXT_DOC_ID_KEY);
            final Value data = broker.collectionsDb.get(key);
            if (data != null) {
                nextDocId = ByteConversion.byteToInt(data.getData(), 0);
                ++nextDocId;
                if (nextDocId == 0x7FFFFFFF) {
                    broker.getDatabase().setReadOnly();
                    throw new IOException("Max. number of document ids reached. Database is set to " +
                                    "read-only state. Please do a complete backup/restore to compact the db and " +
                                    "free document ids.");
                }
            }
            final byte[] d = new byte[4];
            ByteConversion.intToByte(nextDocId, d, 0);
            broker.collectionsDb.put(txn, key, d, true);

        } catch (final LockException e) {
            LOG.warn("Failed to acquire lock on " + broker.collectionsDb.getFile().getName(), e);
            //TODO : rethrow ? -pb
        } finally {
            lock.release(Lock.WRITE_LOCK);
        }
        return nextDocId;
    }
    
    /** set user-defined Reader */
    public void setReader(final XMLReader reader){
        userReader = reader;
    }

    /** 
     * Get xml reader from readerpool and setup validation when needed.
     */
    protected final XMLReader getReader(final DBBroker broker, final boolean validation, final CollectionConfiguration colconfig) {
        // If user-defined Reader is set, return it;
        if (userReader != null) {
            return userReader;
        }
        // Get reader from readerpool.
        final XMLReader reader = broker.getBrokerPool().getParserPool().borrowXMLReader();
        
        // If Collection configuration exists (try to) get validation mode
        // and setup reader with this information.
        if (!validation) {
            XMLReaderObjectFactory.setReaderValidationMode(XMLReaderObjectFactory.VALIDATION_SETTING.DISABLED, reader);
            
        } else if( colconfig!=null ) {
            final VALIDATION_SETTING mode = colconfig.getValidationMode();
            XMLReaderObjectFactory.setReaderValidationMode(mode, reader);
        }
        // Return configured reader.
        return reader;
    }

    /**
     * Reset validation mode of reader and return reader to reader pool.
     */    
    protected final void releaseReader(final DBBroker broker, final org.exist.collections.IndexInfo info, final XMLReader reader) {
        if(userReader != null){
            return;
        }
        
        if(info.getIndexer().getDocSize() > POOL_PARSER_THRESHOLD) {
            return;
        }
        
        // Get validation mode from static configuration
        final Configuration config = broker.getConfiguration();
        final String optionValue = (String) config.getProperty(XMLReaderObjectFactory.PROPERTY_VALIDATION_MODE);
        final VALIDATION_SETTING validationMode = XMLReaderObjectFactory.convertValidationMode(optionValue);
        
        // Restore default validation mode
        XMLReaderObjectFactory.setReaderValidationMode(validationMode, reader);
        
        // Return reader
        broker.getBrokerPool().getParserPool().returnXMLReader(reader);
    }
    
    //statistics
    /**
     * Returns the estimated amount of memory used by this collection
     * and its documents. This information is required by the
     * {@link org.exist.storage.CollectionCacheManager} to be able
     * to resize the caches.
     *
     * @return estimated amount of memory in bytes
     */
    public int getMemorySize() {
        return SHALLOW_SIZE + documents.size() * DOCUMENT_SIZE;
    }

    //Deprecated methods or methods to refactoring
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
        this.permissions = permissions;
    }

    /*** TODO why do we need this? is it just for the versioning trigger?
     * If so we need to enable/disable specific triggers!
     ***/
    @Deprecated
    public void setTriggersEnabled(final boolean enabled) {
        try {
            getLock().acquire(Lock.WRITE_LOCK);
            this.triggersEnabled = enabled;
        } catch(final LockException e) {
            LOG.warn(e.getMessage(), e);
            //Ouch ! -pb
            this.triggersEnabled = enabled;
        } finally {
            getLock().release(Lock.WRITE_LOCK);
        }
    }

    //classes & interfaces
    private interface ValidateBlock {
        public void run(IndexInfo info) throws SAXException, EXistException;
    }

    public abstract class CollectionEntry {
        private final XmldbURI uri;
        private Permission permissions;
        private long created = -1;

        protected CollectionEntry(final XmldbURI uri, final Permission permissions) {
            this.uri = uri;
            this.permissions = permissions;
        }
        
        public abstract void readMetadata(DBBroker broker);
        public abstract void read(VariableByteInput is) throws IOException;

        public XmldbURI getUri() {
            return uri;
        }
        
        public long getCreated() {
            return created;
        }

        protected void setCreated(final long created) {
            this.created = created;
        }

        public Permission getPermissions() {
            return permissions;
        }

        protected void setPermissions(final Permission permissions) {
            this.permissions = permissions;
        }

    }

    public class SubCollectionEntry extends CollectionEntry {

        public SubCollectionEntry(final XmldbURI uri) {
            super(uri, PermissionFactory.getDefaultCollectionPermission());
        }

        @Override
        public void readMetadata(final DBBroker broker) {
            broker.readCollectionEntry(this);
        }

        @Override
        public void read(final VariableByteInput is) throws IOException {
            is.skip(1);
            final int collLen = is.readInt();
            for(int i = 0; i < collLen; i++) {
                is.readUTF();
            }
            getPermissions().read(is);
            setCreated(is.readLong());
        }

        public void read(final Stored collection) {
            setPermissions(collection.permissions());
            setCreated(collection.getCreationTime());
        }
    }

    public class DocumentEntry extends CollectionEntry {

        public DocumentEntry(final DocumentImpl document) {
            super(document.getURI(), document.getPermissions());
            setCreated(document.getMetadata().getCreated());
        }

        @Override
        public void readMetadata(final DBBroker broker) {
        }

        @Override
        public void read(final VariableByteInput is) throws IOException {
        }
    }
}
