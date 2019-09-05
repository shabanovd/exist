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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.exist.storage.BrokerPool;
import org.exist.storage.CacheManager;
import org.exist.storage.cache.Cacheable;
import org.exist.xmldb.XmldbURI;

/**
 * Global cache for {@link org.exist.collections.Collection} objects. The
 * cache is owned by {@link org.exist.storage.index.CollectionStore}. It is not
 * synchronized. Thus a lock should be obtained on the collection store before
 * accessing the cache.
 * 
 * @author wolf
 */
public class CollectionCache implements org.exist.storage.cache.Cache {
    
    String fileName = "collection cache";
    CacheManager cacheManager = null;

    Cache<String, Collection> names = Caffeine.newBuilder()
        .initialCapacity(10_000)
//        .softValues()
//        .weakValues()
//        .removalListener(new RemovalListener<String, Collection>() {
//            @Override
//            public void onRemoval(String url, Collection col, RemovalCause removalCause) {
//                final Lock lock = col.getLock();
//                if (lock.attempt(Lock.READ_LOCK)) {
//                    try {
//                        if (col.allowUnload()) {
//                            if (pool.getConfigurationManager() != null) { // might be null during db initialization
//                                pool.getConfigurationManager().invalidate(col.getURI(), null);
//                            }
//                            col.sync(true);
//                        }
//                    } finally {
//                        lock.release(Lock.READ_LOCK);
//                    }
//                }
//            }
//        })
        .build();
    
    private BrokerPool pool;

    public CollectionCache(BrokerPool pool, int blockBuffers, double growthThreshold) {
        this.pool = pool;
    }

    @Override
    public String getType() {
        return CacheManager.DATA_CACHE;
    }

    public void add(Cacheable item) {
        add(item, 1);
    }

    public void add(Cacheable item, int initialRefCount) {
        if (item instanceof Collection) {
            Collection col = (Collection) item;
            final String name = col.getURI().getRawCollectionPath();
            names.put(name, col);
        }
    }

    public Cacheable get(Cacheable item) {
        if (item instanceof Collection) {
            Collection col = (Collection) item;

            return get(col.getURI());
        }
        
        return null;
    }

    public Collection get(XmldbURI name) {
        return names.getIfPresent(name.getRawCollectionPath());
    }

    @Override
    public Cacheable get(long key) {
        return null;
    }

    public void remove(Cacheable item) {
        if (item instanceof Collection) {
            final Collection col = (Collection) item;
            names.invalidate(col.getURI().getRawCollectionPath());

            // might be null during db initialization
            CollectionConfigurationManager cm = pool.getConfigurationManager();
            if (cm != null) {
                cm.invalidate(col.getURI(), null);
            }
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
        for (final Collection collection : names.asMap().values()) {
            if (collection != null) {
                size += collection.getMemorySize();
            }
        }
        return size;
    }

    public boolean flush() {
//        boolean flushed = false;
//        for (Collection col : names.asMap().values()) {
//            if (col.isDirty()) {
//                flushed = flushed | col.sync(false);
//            }
//        }
//        return flushed;
        return false;
    }


    /* (non-Javadoc)
     * @see org.exist.storage.cache.Cache#hasDirtyItems()
     */
    public boolean hasDirtyItems() {
//        for (Collection col : names.asMap().values()) {
//            if (col.isDirty()) {
//                return true;
//            }
//        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.exist.storage.cache.Cache#getBuffers()
     */
    public int getBuffers() {
        return (int) names.estimatedSize();
    }

    /* (non-Javadoc)
     * @see org.exist.storage.cache.Cache#getUsedBuffers()
     */
    public int getUsedBuffers() {
        return (int) names.estimatedSize();
    }

    /* (non-Javadoc)
     * @see org.exist.storage.cache.Cache#getHits()
     */
    public int getHits() {
        return (int) names.stats().hitCount();
    }

    /* (non-Javadoc)
     * @see org.exist.storage.cache.Cache#getFails()
     */
    public int getFails() {
        return (int) names.stats().missCount();
    }

//    public int getThrashing() {
//        return accounting.getThrashing();
//    }

    /* (non-Javadoc)
     * @see org.exist.storage.cache.Cache#setFileName(java.lang.String)
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    /* (non-Javadoc)
     * @see org.exist.storage.cache.Cache#getGrowthFactor()
     */
    public double getGrowthFactor() {
        return 1.0;
    }

    /* (non-Javadoc)
     * @see org.exist.storage.cache.Cache#setCacheManager(org.exist.storage.CacheManager)
     */
    public void setCacheManager(CacheManager manager) {
        this.cacheManager = manager;
    }

    /* (non-Javadoc)
     * @see org.exist.storage.cache.Cache#resize(int)
     */
    public void resize(int newSize) {
    }

    public int getLoad() {
        return (int) names.stats().loadCount();
//        if (hitsOld == 0) {
//            hitsOld = accounting.getHits();
//            return Integer.MAX_VALUE;
//        }
//        final int load = accounting.getHits() - hitsOld;
//        hitsOld = accounting.getHits();
//        return load;
    }
}
