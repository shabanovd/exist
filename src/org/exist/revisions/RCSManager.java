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
package org.exist.revisions;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

import org.apache.log4j.Logger;
import org.exist.*;
import org.exist.EventListener;
import org.exist.plugin.PluginsManager;
import org.exist.security.*;
import org.exist.storage.DBBroker;
import org.exist.storage.md.MetaData;
import org.exist.storage.serializers.EXistOutputKeys;
import org.exist.storage.serializers.Serializer;

import javax.xml.transform.OutputKeys;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class RCSManager {

    protected final static Logger LOG = Logger.getLogger( RCSManager.class );

    private static final String DIR_NAME = "organizations";
    private static final String INDEX_DIR_NAME = "RCS";
    
    private static RCSManager instance = null;
    
    public static RCSManager get() {
        return instance;
    }
    
    Database db;
    PluginsManager manager;
    MetaData md;

    Path data_folder;

    boolean noDots = false;

    List<EventListener<CommitLog>> commitsListener = new ArrayList<>();

    Map<Thread, CommitWriter> activeCommits = new HashMap<>();

    protected Map<String, RCSHolder> holders = new HashMap<>();

    public RCSManager(PluginsManager manager) throws PermissionDeniedException, IOException {

        this.manager = manager;

        db = manager.getDatabase();

        md = MetaData.get();
        
        try  {
            getDataDir().resolve(":");
        } catch (InvalidPathException e) {
            noDots = true;
        }

        open();

        instance = this;
    }

    private void open() throws IOException {
        data_folder = getDataDir().resolve(DIR_NAME);

        Files.createDirectories(data_folder);

        for (Path orgPath : Files.newDirectoryStream(data_folder)) {
            if (!Files.isDirectory(orgPath)) continue;

            String org_folder = orgPath.getFileName().toString();

            Path indexPath = orgPath.resolve(INDEX_DIR_NAME);
            if (Files.notExists(indexPath)) continue;

            holders.put(org_folder, new RCSHolder(this, indexPath));
        }
    }

    private Path getDataDir() {
        return db.getActiveBroker().getDataFolder().toPath();
    }

    public synchronized RCSHolder getHolder(String oid) {
        return holders.get(oid);
    }

    public synchronized RCSHolder getOrCreateHolder(String oid) throws IOException {

        RCSHolder holder = holders.get(oid);

        if (holder == null) {
            Path path = data_folder.resolve(oid).resolve(INDEX_DIR_NAME);

            Files.createDirectories(path);

            holder = new RCSHolder(this, path);

            holders.put(oid, holder);
        }

//        if (holder == null) throw new IOException("Organization '"+org+"' don't have lucene index.");

        return holder;
    }

    public CommitWriter activeCommit() {
        return activeCommits.get(Thread.currentThread());
    }

    protected CommitWriter addCommit(CommitWriter commit) {
        Thread cur = Thread.currentThread();

        if (activeCommits.get(cur) != null) throw new RuntimeException("only one active commit allow");

        activeCommits.put(cur, commit);

        return commit;

    }

    protected void removeCommit(CommitLog commitLog) {
        CommitWriter active = activeCommits.remove(Thread.currentThread());
        if (active != commitLog) LOG.error("commitLog different from active one");
    }

    protected Serializer serializer(DBBroker broker) {
        Serializer serializer = broker.getSerializer();
        serializer.setUser(broker.getSubject());
        try {
            serializer.setProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
            serializer.setProperty(EXistOutputKeys.OUTPUT_DOCTYPE, "yes");

        } catch (Exception e) {}
        
        return serializer;
    }

    public boolean registerCommitsListener(EventListener<CommitLog> listener) {
        return commitsListener.add(listener);
    }

    public boolean unregisterCommitsListener(EventListener<CommitLog> listener) {
        return commitsListener.remove(listener);
    }
}
