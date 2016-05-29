/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2016 The eXist Project
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
package org.expath.exist;

import org.exist.dom.persistent.BinaryDocument;
import org.exist.dom.persistent.DocumentImpl;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.xmldb.XmldbURI;

import java.io.IOException;
import java.util.zip.ZipInputStream;

public class ZipFileFromDb implements ZipFileSource {

    private DBBroker broker;
    private BinaryDocument binaryDoc = null;
    private final XmldbURI uri;

    public ZipFileFromDb(DBBroker broker, XmldbURI uri) {
        this.broker = broker;
        this.uri = uri;
    }

    @Override
    public ZipInputStream getStream() throws IOException, PermissionDeniedException {

        if (binaryDoc == null) {
            binaryDoc = getDoc();
        }

        return new ZipInputStream(broker.getBinaryResource(binaryDoc));
    }

    @Override
    public void close() {
        if (binaryDoc != null) {
            binaryDoc.getUpdateLock().release(Lock.READ_LOCK);
        }
    }

    private BinaryDocument getDoc() throws PermissionDeniedException {

        DocumentImpl doc = broker.getXMLResource(uri, Lock.READ_LOCK);
        if (doc == null || doc.getResourceType() != DocumentImpl.BINARY_FILE) {
            if (doc != null) {
                doc.getUpdateLock().release(Lock.READ_LOCK);
            }
            return null;
        }

        return (BinaryDocument) doc;
    }
}
