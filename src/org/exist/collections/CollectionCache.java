/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2013 The eXist Project
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

import java.util.Iterator;

import org.exist.Database;
import org.exist.storage.CacheManager;
import org.exist.storage.cache.Cacheable;
import org.exist.storage.cache.LRUCache;
import org.exist.storage.lock.Lock;
import org.exist.storage.lock.MultiReadReentrantLock;
import org.exist.util.hashtable.Object2LongHashMap;
import org.exist.util.hashtable.SequencedLongHashMap;
import org.exist.xmldb.XmldbURI;

/**
 * Global cache for {@link org.exist.collections.Collection} objects. The
 * cache is owned by {@link org.exist.storage.index.CollectionStore}. It is not
 * synchronized. Thus a lock should be obtained on the collection store before
 * accessing the cache.
 * 
 * @author wolf
 */
public class CollectionCache extends LRUCache<Collection> {

    private Object2LongHashMap<String> names;
    private Database db;
    private Lock lock;

    public CollectionCache(Database db, int blockBuffers, double growthThreshold) {
        super(blockBuffers, 2.0, 0.000001, CacheManager.DATA_CACHE);
        this.names = new Object2LongHashMap<String>(blockBuffers);
        this.db = db;
        
        lock = new MultiReadReentrantLock("collection cache");
        setFileName("collection cache");
    }
    
    public Lock getLock() {
        return lock;
    }
    
    public void add(Collection collection) {
        add(collection, 1);
    }

    public void add(Collection collection, int initialRefCount) {
        if(db.isInitializing()) {
            return;
        }
        
        super.add(collection, initialRefCount);
        
        final String name = collection.getURI().getRawCollectionPath();
        names.put(name, collection.getKey());
    }

    public Collection get(Collection collection) {
        return get(collection.getKey());
    }

    public Collection get(XmldbURI name) {
        final long key = names.get(name.getRawCollectionPath());
        if (key < 0) {
            return null;
        }
        return get(key);
    }

    /**
     * Overwritten to lock collections before they are removed.
     */
    protected void removeOne(Cacheable item) {
        boolean removed = false;
        SequencedLongHashMap.Entry<Collection> next = map.getFirstEntry();
        do {
            final Collection cached = next.getValue();
            if(cached.getKey() != item.getKey()) {
                final Collection old = cached;
                final Lock lock = old.getLock();
                if (lock.attempt(Lock.READ_LOCK)) {
                    try {
                        if (cached.allowUnload()) {
                            if(db.getConfigurationManager()!=null) { // might be null during db initialization
                                db.getConfigurationManager().invalidate(old.getURI());
                            }
                            names.remove(old.getURI().getRawCollectionPath());
                            cached.sync(true);
                            map.remove(cached.getKey());
                            removed = true;
                        }
                    } finally {
                        lock.release(Lock.READ_LOCK);
                    }
                }
            } else {
                next = next.getNext();
                if(next == null) {
                    LOG.info("Unable to remove entry");
                    next = map.getFirstEntry();
                }
            }
        } while(!removed);
        cacheManager.requestMem(this);
    }

    public void remove(Collection item) {
        super.remove(item);

        names.remove(item.getURI().getRawCollectionPath());
        
        // might be null during db initialization
        CollectionConfigurationManager manager = db.getConfigurationManager();
        if(manager != null) {
            manager.invalidate(item.getURI());
        }
    }

    /**
     * Compute and return the in-memory size of all collections
     * currently contained in this cache.
     *
     * @see org.exist.storage.CollectionCacheManager
     * @return in-memory size in bytes.
     */
    public int getRealSize() {
        int size = 0;
        for (final Iterator<Long> i = names.valueIterator(); i.hasNext(); ) {
            final Collection collection = get(i.next());
            if (collection != null) {
                size += collection.getMemorySize();
            }
        }
        return size;
    }

    public void resize(int newSize) {
        if (newSize < max) {
            shrink(newSize);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Growing collection cache to " + newSize);
            }
            SequencedLongHashMap<Collection> newMap = new SequencedLongHashMap<Collection>(newSize * 2);
            Object2LongHashMap<String> newNames = new Object2LongHashMap<String>(newSize);
            SequencedLongHashMap.Entry<Collection> next = map.getFirstEntry();
            Collection cacheable;
            while(next != null) {
                cacheable = next.getValue();
                newMap.put(cacheable.getKey(), cacheable);
                newNames.put(cacheable.getURI().getRawCollectionPath(), cacheable.getKey());
                next = next.getNext();
            }
            max = newSize;
            map = newMap;
            names = newNames;
            accounting.reset();
            accounting.setTotalSize(max);
        }
    }

    @Override
    protected void shrink(int newSize) {
        super.shrink(newSize);
        names = new Object2LongHashMap<String>(newSize);
    }
}
