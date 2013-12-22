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
package org.exist.storage.cache;

import org.exist.storage.CacheManager;
import org.exist.util.hashtable.SequencedLongHashMap;

/**
 * A simple cache implementing a Last Recently Used policy. This cache
 * implementation is based on a
 * {@link org.exist.util.hashtable.SequencedLongHashMap}. Contrary to the other
 * {@link org.exist.storage.cache.Cache} implementations, LRUCache ignores
 * reference counts or timestamps.
 * 
 * @author wolf
 */
public class LRUCache<T extends Cacheable> implements Cache<T> {

    protected int max;
    protected SequencedLongHashMap<T> map;

    protected Accounting accounting;

    protected volatile long hitsOld = -1;

    protected double growthFactor;

    protected String fileName;

    protected CacheManager cacheManager = null;

    private String type;

    public LRUCache(int size, double growthFactor, double growthThreshold, String type) {
        max = size;
        this.growthFactor = growthFactor;
        map = new SequencedLongHashMap<T>(size * 2);
        accounting = new Accounting(growthThreshold);
        accounting.setTotalSize(max);
        this.type = type;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#add(org.exist.storage.cache.Cacheable,
     * int)
     */
    public void add(T item, int initialRefCount) {
        add(item);
    }

    public String getType() {
        return type;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#add(org.exist.storage.cache.Cacheable)
     */
    public void add(T item) {
        if (map.size() == max) {
            removeOne(item);
        }
        map.put(item.getKey(), item);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#get(org.exist.storage.cache.Cacheable)
     */
    public T get(T item) {
        return get(item.getKey());
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#get(long)
     */
    public T get(long key) {
        final T obj = map.get(key);
        if (obj == null) {
            accounting.missesIncrement();
        } else {
            accounting.hitIncrement();
        }
        return obj;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.exist.storage.cache.Cache#remove(org.exist.storage.cache.Cacheable)
     */
    public void remove(T item) {
        map.remove(item.getKey());
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#flush()
     */
    public boolean flush() {
        boolean flushed = false;
        T cacheable;
        SequencedLongHashMap.Entry<T> next = map.getFirstEntry();
        while (next != null) {
            cacheable = next.getValue();
            if (cacheable.isDirty()) {
                flushed = flushed | cacheable.sync(false);
            }
            next = next.getNext();
        }
        return flushed;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#hasDirtyItems()
     */
    public boolean hasDirtyItems() {
        T cacheable;
        SequencedLongHashMap.Entry<T> next = map.getFirstEntry();
        while (next != null) {
            cacheable = next.getValue();
            if (cacheable.isDirty()) {
                return true;
            }
            next = next.getNext();
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#getBuffers()
     */
    public int getBuffers() {
        return max;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#getUsedBuffers()
     */
    public int getUsedBuffers() {
        return map.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#getHits()
     */
    public long getHits() {
        return accounting.getHits();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#getFails()
     */
    public long getFails() {
        return accounting.getMisses();
    }

    public int getThrashing() {
        return accounting.getThrashing();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#setFileName(java.lang.String)
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }

    protected void removeOne(Cacheable item) {
        boolean removed = false;
        SequencedLongHashMap.Entry<T> next = map.getFirstEntry();
        do {
            final T cached = next.getValue();
            if (cached.allowUnload() && cached.getKey() != item.getKey()) {
                cached.sync(true);
                map.remove(next.getKey());
                removed = true;
            } else {
                next = next.getNext();
                if (next == null) {
                    LOG.debug("Unable to remove entry");
                    next = map.getFirstEntry();
                }
            }
        } while (!removed);
        accounting.replacedPage(item);
        if (growthFactor > 1.0 && accounting.resizeNeeded()) {
            cacheManager.requestMem(this);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#getGrowthFactor()
     */
    public double getGrowthFactor() {
        return growthFactor;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.exist.storage.cache.Cache#setCacheManager(org.exist.storage.CacheManager
     * )
     */
    public void setCacheManager(CacheManager manager) {
        this.cacheManager = manager;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.exist.storage.cache.Cache#resize(int)
     */
    public void resize(int newSize) {
        if (newSize < max) {
            shrink(newSize);
        } else {
            SequencedLongHashMap<T> newMap = new SequencedLongHashMap<T>(newSize * 2);
            SequencedLongHashMap.Entry<T> next = map.getFirstEntry();
            T cacheable;
            while (next != null) {
                cacheable = next.getValue();
                newMap.put(cacheable.getKey(), cacheable);
                next = next.getNext();
            }
            max = newSize;
            map = newMap;
            accounting.reset();
            accounting.setTotalSize(max);
        }
    }

    protected void shrink(int newSize) {
        flush();
        this.map = new SequencedLongHashMap<T>(newSize);
        this.max = newSize;
        accounting.reset();
        accounting.setTotalSize(max);
    }

    public long getLoad() {
        if (hitsOld == 0) {
            hitsOld = accounting.getHits();
            return Long.MAX_VALUE;
        }
        final long load = accounting.getHits() - hitsOld;
        hitsOld = accounting.getHits();
        return load;
    }
}
