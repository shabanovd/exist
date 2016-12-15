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
package org.exist.storage.md;

import org.exist.Namespaces;
import org.exist.backup.restore.listener.RestoreListener;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.Attributes;

import java.util.List;

//TODO: import static org.exist.storage.md.MDStorageManager.KEY;
//TODO: import static org.exist.storage.md.MDStorageManager.UUID;
//TODO: import static org.exist.storage.md.MDStorageManager.VALUE;
import static org.exist.storage.md.MetaData.NAMESPACE_URI;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class RestoreMetadata {

    public final static String UUID = "uuid";
    public final static String META = "meta";
    public final static String KEY = "key";
    public final static String VALUE = "value";

    MetaData md;

    String colUUID = null;
    String docUUID = null;

    public XmldbURI colURL = null;
    public XmldbURI docURL = null;

    Metas colMetas = null;
    Metas docMetas = null;

    public List<Pair> colPairs = null;
    public List<Pair> docPairs = null;

    public RestoreMetadata(MetaData md) {
        this.md = md;
    }

    public boolean isChecksumSame(RestoreListener listener) {
        if (docMetas == null || docPairs == null || docPairs.isEmpty()) {
            if (docMetas == null || docPairs == null) {
                listener.info("docMetas: "+docMetas+"; docPairs: "+docPairs+"; url: "+docMetas.getURI());
            }
            return false;
        }

        String backupHash = "";
        String databaseHash = "";

        for (Pair pair : docPairs) {
            if ("normalized-checksum".equals(pair.key)) {
                if (pair.value != null) {
                    backupHash = pair.value;
                }
                break;
            }
        }

        Meta meta = docMetas.get("normalized-checksum");
        if (meta != null && meta.getValue() != null) {
            databaseHash = meta.getValue().toString();
        }

        boolean flag = !backupHash.isEmpty() && !databaseHash.isEmpty() && backupHash.equals(databaseHash);
        if (flag) {
            listener.info("backupHash: " + backupHash + "; databaseHash: " + databaseHash + "; url: " + docMetas.getURI());
        }

        return flag;
    }

    public void restoreMetadata(Metas metas, List<Pair> pairs) {

        main_loop:
        for (Meta meta : metas.metas()) {
            String uuid = meta.getUUID();

            for (Pair pair : pairs) {
                if (uuid.equals(pair.uuid)) {
                    continue main_loop;
                }
            }

            meta.delete();
        }

        for (Pair pair : pairs) {
            restoreMetadata(metas, pair);
        }
    }

    public void restoreMetadata(Metas metas, Pair pair) {

        Meta meta = md.getMeta(pair.uuid);

        if (meta == null) {

            Meta cur = metas.get(pair.key);
            if (cur == null) {
                md._addMeta(metas, pair.uuid, pair.key, pair.value);

            } else if (!pair.value.equals(cur.getValue())) {
                metas.put(pair.key, pair.value);
            }

        } else if (!meta.getObject().equals(metas.getUUID())) {
            meta.delete();

            Meta cur = metas.get(pair.key);
            if (cur == null) {
                md._addMeta(metas, pair.uuid, pair.key, pair.value);

            } else if (!pair.value.equals(cur.getValue())) {
                metas.put(pair.key, pair.value);

            }
            //TODO: check pair uuid?

        } else if (!pair.value.equals(meta.getValue())) {

            metas.put(pair.key, pair.value);
        }
    }

    public String startElement(String namespaceURI, String localName, String qName, Attributes atts) {
        if (Namespaces.EXIST_NS.equals(namespaceURI) ) {
            if (localName.equals("collection")) {
                colUUID = atts.getValue(NAMESPACE_URI, UUID);
                if (colUUID != null) {
                    colMetas = md.getMetas(colUUID);
                }
                return colUUID;

            } else if (localName.equals("resource")) {
                docUUID = atts.getValue(NAMESPACE_URI, UUID);
                if (docUUID != null) {
                    docMetas = md.getMetas(docUUID);
                }
                return docUUID;
            }
        } else if (META.equals(localName) && NAMESPACE_URI.equals(namespaceURI)) {
            String uuid = atts.getValue(NAMESPACE_URI, UUID);
            String key = atts.getValue(NAMESPACE_URI, KEY);
            String value = atts.getValue(NAMESPACE_URI, VALUE);

            if (docPairs != null) {
                docPairs.add(new RestoreMetadata.Pair(uuid, key, value));
            } else if (colMetas != null) {
                colPairs.add(new RestoreMetadata.Pair(uuid, key, value));
            }
        }
        return null;
    }

    public void endElement(String namespaceURI, String localName, String qName) {
        if (Namespaces.EXIST_NS.equals(namespaceURI) ) {
            if (localName.equals("collection")) {
                //check collection
                if (colMetas == null && colURL != null) {
                    colMetas = md.getMetas(colURL);
                    if (colMetas != null) {
                        if (!colMetas.getUUID().equals(colUUID)) {
                            colMetas.delete();
                            colMetas = null;
                        }
                    }
                    if (colMetas == null) {
                        if (colUUID == null) {
                            colMetas = md._addMetas(colURL);
                        } else {
                            colMetas = md._addMetas(colURL.toString(), colUUID);
                        }
                    }
                }
                if (colMetas != null) {
                    restoreMetadata(colMetas, colPairs);
                }

                colUUID = null;
                docUUID = null;

                colURL = null;
                docURL = null;

                colMetas = null;
                docMetas = null;

            } else if (localName.equals("resource")) {
                //check document
                if (docMetas == null && docURL != null) {
                    docMetas = md.getMetas(docURL);
                    if (docMetas != null) {
                        if (!docMetas.getUUID().equals(docUUID)) {
                            docMetas.delete();
                            docMetas = null;
                        }
                    }
                    if (docMetas == null) {
                        if (docUUID == null) {
                            docMetas = md._addMetas(docURL);
                        } else {
                            docMetas = md._addMetas(docURL.toString(), docUUID);
                        }
                    }
                }
                if (docMetas != null) {
                    restoreMetadata(docMetas, docPairs);
                }

                docUUID = null;

                docURL = null;

                docMetas = null;
            }
        }
    }

    public static class Pair {
        String uuid;
        String key;
        String value;

        public Pair(String uuid, String key, String value) {
            this.uuid = uuid;
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "["+uuid+"] "+key+": "+value;
        }
    }
}
