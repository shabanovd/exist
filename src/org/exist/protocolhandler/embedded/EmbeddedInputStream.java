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
 * $Id: EmbeddedInputStream.java 223 2007-04-21 22:13:05Z dizzzz $
 */

package org.exist.protocolhandler.embedded;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.exist.collections.Collection;
import org.exist.dom.BinaryDocument;
import org.exist.dom.DocumentImpl;
import org.exist.protocolhandler.xmldb.XmldbURL;
import org.exist.security.Subject;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.storage.serializers.EXistOutputKeys;
import org.exist.storage.serializers.Serializer;
import org.exist.xmldb.XmldbURI;

/**
 * Read document from embedded database as a (input)stream.
 */
public class EmbeddedInputStream extends InputStream {
    
    private final static Logger logger = LogManager.getLogger(EmbeddedInputStream.class);

    Path tmpFile = null;
    InputStream bis = null;
    
    /**
     *  Constructor of EmbeddedInputStream. 
     * 
     * @param xmldbURL Location of document in database.
     * @throws MalformedURLException Thrown for illegalillegal URLs.
     */
    public EmbeddedInputStream(XmldbURL xmldbURL) throws IOException {
        
        this(null, xmldbURL);
    }

        /**
     *  Constructor of EmbeddedInputStream.
     *
     * @param url Location of document in database.
     * @throws MalformedURLException Thrown for illegalillegal URLs.
     */
    public EmbeddedInputStream(BrokerPool brokerPool, XmldbURL url) throws IOException {

        logger.debug("Initializing EmbeddedInputStream");

        try {
            BrokerPool pool = BrokerPool.getInstance(url.getInstanceName());

            final XmldbURI path = XmldbURI.create(url.getPath());

            Subject user;
            if (url.hasUserInfo()) {
                user = EmbeddedUser.authenticate(url, pool);
                if (user == null) {
                    throw new IOException("Unauthorized user "+url.getUsername());
                }

            } else {
                user = EmbeddedUser.getUserGuest(pool);
            }

            Collection collection = null;
            DocumentImpl resource = null;

            try (DBBroker broker = pool.get(user)) {

                resource = broker.getXMLResource(path, Lock.READ_LOCK);

                if (resource == null) {
                    // Test for collection
                    collection = broker.openCollection(path, Lock.READ_LOCK);
                    if (collection == null) {
                        // No collection, no document
                        throw new IOException("Resource " + url.getPath() + " not found.");

                    } else {
                        // Collection
                        throw new IOException("Resource " + url.getPath() + " is a collection.");
                    }

                } else {
                    tmpFile = Files.createTempFile("EMBEDDED-download-", ".tmp");

                    try (OutputStream os = Files.newOutputStream(tmpFile)) {

                        if (resource.getResourceType() == DocumentImpl.XML_FILE) {
                            final Serializer serializer = broker.getSerializer();
                            serializer.reset();

                            // Preserve doctype
                            serializer.setProperty(EXistOutputKeys.OUTPUT_DOCTYPE, "yes");
                            try (final Writer w = new OutputStreamWriter(os, "UTF-8")) {
                                serializer.serialize(resource, w);
                            }

                        } else {
                            broker.readBinaryResource((BinaryDocument) resource, os);
                        }
                    }
                }
            } finally {
                if (resource != null){
                    resource.getUpdateLock().release(Lock.READ_LOCK);
                }

                if (collection != null){
                    collection.release(Lock.READ_LOCK);
                }
            }
        } catch (Exception e) {
            throw new IOException("can't get db for "+url, e);
        }

        if (tmpFile == null) throw new IOException("no data for "+url);

        bis = Files.newInputStream(tmpFile);

        logger.debug("Initializing EmbeddedInputStream done");
    }
    
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return bis.read(b, off, len);
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        return bis.read(b, 0, b.length);
    }
    
    @Override
    public long skip(long n) throws IOException {
        return bis.skip(n);
    }
    
    @Override
    public void reset() throws IOException {
        bis.reset();
    }
    
    @Override
    public int read() throws IOException {
        return bis.read();
    }

    @Override
    public void close() throws IOException {
        try {
            bis.close();
        } finally {
            Files.deleteIfExists(tmpFile);
        }
    }

    @Override
    public int available() throws IOException {
        return bis.available();
    }

}
