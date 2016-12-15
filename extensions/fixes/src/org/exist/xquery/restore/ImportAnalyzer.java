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

import org.exist.backup.BackupDescriptor;
import org.exist.backup.restore.listener.RestoreListener;
import org.exist.collections.Collection;
import org.exist.dom.DocumentImpl;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.DBBroker;
import org.exist.storage.md.MetaData;
import org.exist.storage.md.Metas;
import org.exist.xmldb.XmldbURI;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class ImportAnalyzer {

    private final static String COL = "collection";
    private final static String DOC = "document";

    private DBBroker broker;

    private org.exist.backup.RestoreHandler rh;

    protected final RestoreListener listener;
    private final String dbBaseUri;

    int items = 1000000;

    //handler state
    protected Map<String, String> uuidToUrl = new LinkedHashMap<>(items);
    protected Map<String, String> colToUuid = new HashMap<>(items);
    protected Map<String, String> docToUuid = new HashMap<>(items);

    protected Map<String, String> parentOfUuid = new LinkedHashMap<>(items);

    protected Set<String> createCols = new LinkedHashSet<>(items);
    protected Set<String> createDocs = new LinkedHashSet<>(items);

    protected Map<String, String> moveCols = new LinkedHashMap<>(10000);
    protected Map<String, String> moveDocs = new LinkedHashMap<>(10000);

//    protected Set<String> moveSafelyCols = new LinkedHashSet<>(10000);
//    protected Set<String> moveSafelyDocs = new LinkedHashSet<>(10000);

    protected Set<String> deleteCols = new LinkedHashSet<>(500000);
    protected Set<String> deleteDocs = new LinkedHashSet<>(500000);

    protected Set<String> replace = new LinkedHashSet<>(10000);

    protected AtomicLong createCOL = new AtomicLong();
    protected AtomicLong createXML = new AtomicLong();
    protected AtomicLong createBIN = new AtomicLong();

    protected AtomicLong skippedXML = new AtomicLong();
    protected AtomicLong skippedBIN = new AtomicLong();

    protected AtomicLong updatedXML = new AtomicLong();
    protected AtomicLong updatedBIN = new AtomicLong();

    boolean stopError = false;


    public ImportAnalyzer(DBBroker broker, RestoreListener listener, String dbBaseUri) {
        this.broker = broker;
        this.listener = listener;
        this.dbBaseUri = dbBaseUri;

        rh = broker.getDatabase().getPluginsManager().getRestoreHandler();
    }

    public BackupHandler handler(BackupDescriptor descriptor) {
        return new BackupHandler(this, descriptor);
    }

    public void analysis() throws PermissionDeniedException {
        MetaData md = MetaData.get();

        listener.info("scan database");
        analysis(broker.getCollection(XmldbURI.DB));

        listener.info("scan backup");
        for (Map.Entry<String, String> entry : uuidToUrl.entrySet()) {
            String uuid = entry.getKey();
            String url = entry.getValue();

            if (url == null) {
                listener.error("uuid '"+uuid+"' have no mapping to url '"+url+"'");
                continue;
            }

            Metas metas = md.getMetas(uuid);

            if (metas == null) {
                if (colToUuid.containsKey(url)) {
                    createCols.add(url);

                } else if (docToUuid.containsKey(url)) {
                    createDocs.add(url);

                } else {
                    listener.error("[create] unknown type of uuid '"+uuid+"' ["+url+"]");
                    stopError = true;
                }

            } else if (!url.equals(metas.getURI())) {

                if (colToUuid.containsKey(url)) {
                    moveCols.put(metas.getURI(), url);

                } else if (docToUuid.containsKey(url)) {
                    moveDocs.put(metas.getURI(), url);

                } else {
                    listener.error("[move] unknown type of uuid '"+uuid+"' ["+url+"]");
                    stopError = true;
                }
            }
        }

    }

    private void analysis(Collection col) throws PermissionDeniedException {
        if (col == null) return;

        MetaData md = MetaData.get();

        XmldbURI url = col.getURI();
        String cUrl = url.toString();

        Metas metas = md.getMetas(url);
        analysis(COL, metas, cUrl, colToUuid, moveCols, deleteCols);

        //documents
        for(Iterator<DocumentImpl> i = col.iterator(broker); i.hasNext(); ) {
            DocumentImpl doc = i.next();

            metas = md.getMetas(doc.getURI());
            analysis(DOC, metas, doc.getURI().toString(), docToUuid, moveDocs, deleteDocs);
        }

        //collections
        for (Iterator<XmldbURI> i = col.collectionIterator(broker); i.hasNext(); ) {
            XmldbURI next = i.next();

            analysis(broker.getCollection(col.getURI().append(next)));
        }
    }

    private void analysis(String type, Metas metas, String cUrl, Map<String, String> urlToUuid, Map<String, String> move, Set<String> delete) {
        if (metas == null) {
            listener.error("internal error: no metadata for "+type+" '"+cUrl+"'");

        } else {
            String bUrl = uuidToUrl.get(metas.getUUID());

            if (bUrl == null) {
                //nothing at backup

                String bUuid = urlToUuid.get(cUrl);
                if (bUuid == null) {
                    delete.add(cUrl);

                } else {
                    replace.add(cUrl);
                    listener.info("replace "+type+" "+cUrl+" "+bUuid+" vs "+metas.getUUID()+" ?");
                    //TODO
                }
            } else if (!bUrl.equals(cUrl)) {
                String bUuid = urlToUuid.get(cUrl);

                move.put(cUrl, bUrl);
                listener.info("move "+type+" "+cUrl+" "+bUuid+" vs "+metas.getUUID()+" ?");
                //TODO
            }
        }
    }
}