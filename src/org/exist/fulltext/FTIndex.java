/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-07 The eXist Project
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
 *
 * $Id$
 */
package org.exist.fulltext;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;
import org.exist.Database;
import org.exist.backup.RawDataBackup;
import org.exist.indexing.AbstractIndex;
import org.exist.indexing.IndexWorker;
import org.exist.indexing.RawBackupSupport;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.NativeTextEngine;
import org.exist.storage.btree.DBException;
import org.exist.storage.index.BFile;
import org.exist.util.DatabaseConfigurationException;
import org.w3c.dom.Element;

/**
 * Implementation of the full text index. We are currently in a redesign process which is
 * not yet complete. We still have dependencies on FTIndex in the database core. Once
 * these dependencies were removed, FTIndex will be moved into a separate extension module.
 */
public class FTIndex extends AbstractIndex implements RawBackupSupport {

    private final static Logger LOG = Logger.getLogger(FTIndex.class);

    public final static String ID = FTIndex.class.getName();

    public static final String FILE_NAME = "words.dbx";

    private final static String CONFIG_ATTR_FILE = "file";

    private File dataFile;

    private BFile bf;

    public FTIndex() {
        //Nothing to do
    }

    public String getIndexId() {
        return ID;
    }

    @Override
    public void configure(Database db, String dataDir, Element config) throws DatabaseConfigurationException {
        super.configure(db, dataDir, config);
        String fileName = FILE_NAME;
        if (config.hasAttribute(CONFIG_ATTR_FILE))
            {fileName = config.getAttribute(CONFIG_ATTR_FILE);}
        dataFile = new File(dataDir + File.separatorChar + fileName);
    }

    @Override
    public void open() throws DatabaseConfigurationException {
        final double cacheGrowth = NativeTextEngine.DEFAULT_WORD_CACHE_GROWTH;
        final double cacheKeyThresdhold = NativeTextEngine.DEFAULT_WORD_KEY_THRESHOLD;
        final double cacheValueThresHold = NativeTextEngine.DEFAULT_WORD_VALUE_THRESHOLD;
        LOG.debug("Creating '" + dataFile.getName() + "'...");
        try {
            bf = new BFile(getDatabase(), (byte)0, false, dataFile, getDatabase().getCacheManager(),
                cacheGrowth, cacheKeyThresdhold, cacheValueThresHold);
        } catch (final DBException e) {
            throw new DatabaseConfigurationException("Failed to create index file: " + dataFile.getAbsolutePath() + ": " +
                e.getMessage());
        }
    }

    @Override
    public void close() throws DBException {
        bf.close();
    }

    @Override
    public void sync() throws DBException {
        bf.flush();
    }

    @Override
    public void remove() throws DBException {
        bf.closeAndRemove();
        bf = null;
    }

    @Override
    public IndexWorker getWorker(DBBroker broker) {
        //TODO : ensure singleton ? a pool ?
        try {
            return new FTIndexWorker(this, broker);
        } catch (final DatabaseConfigurationException e) {
            LOG.warn("Failed to create index worker for full text index: " + e.getMessage(), e);
        }
        return null;
    }

    @Override
    public boolean checkIndex(DBBroker broker) {
        return false;
    }

    public BFile getBFile() {
        return bf;
    }

    public void backupToArchive(RawDataBackup backup) throws IOException {
        final OutputStream os = backup.newEntry(bf.getFile().getName());
        bf.backupToStream(os);
        backup.closeEntry();
    }
}