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
 * $Id: EmbeddedOutputStream.java 223 2007-04-21 22:13:05Z dizzzz $
 */

package org.exist.protocolhandler.embedded;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.collections.IndexInfo;
import org.exist.dom.DocumentImpl;
import org.exist.protocolhandler.xmldb.XmldbURL;
import org.exist.security.Subject;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock;
import org.exist.storage.txn.Txn;
import org.exist.util.FileInputSource;
import org.exist.util.MimeTable;
import org.exist.util.MimeType;
import org.exist.xmldb.XmldbURI;

/**
 * Write document to local database (embedded) using output stream.
 */
public class EmbeddedOutputStream  extends OutputStream {
    
    private final static Logger logger = LogManager.getLogger(EmbeddedOutputStream.class);

    XmldbURL url;

    Path tmpFile;
    OutputStream bos;
    
    /**
     *  Constructor of EmbeddedOutputStream. 
     * 
     * @param url Location of document in database.
     * @throws MalformedURLException Thrown for illegal URLs.
     */
    public EmbeddedOutputStream(XmldbURL url) throws IOException {
        
        logger.debug("Initializing EmbeddedUploadThread");

        this.url = url;

        tmpFile = Files.createTempFile("EMBEDDED-upload-", ".tmp");

        bos = Files.newOutputStream(tmpFile);

        logger.debug("Initializing EmbeddedUploadThread done");
    }

    
    @Override
    public void write(int b) throws IOException {
        bos.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        bos.write(b,0,b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        bos.write(b,off,len);
    }

    @Override
    public void close() throws IOException {
        Collection collection = null;
        try {
            bos.close();

            BrokerPool pool = BrokerPool.getInstance(url.getInstanceName());

            Subject user;
            if (url.hasUserInfo()) {
                user = EmbeddedUser.authenticate(url, pool);
                if (user == null) {
                    throw new IOException("Unauthorized user "+url.getUsername());
                }

            } else {
                user = EmbeddedUser.getUserGuest(pool);
            }

            try (DBBroker broker = pool.get(user)) {
                final XmldbURI collectionUri = XmldbURI.create(url.getCollection());
                final XmldbURI documentUri = XmldbURI.create(url.getDocumentName());

                collection = broker.openCollection(collectionUri, Lock.READ_LOCK);

                if (collection == null) {
                    throw new IOException("Resource "+collectionUri.toString()+" is not a collection.");
                }

                if (collection.hasChildCollection(broker, documentUri)) {
                    throw new IOException("Resource "+documentUri.toString()+" is a collection.");
                }

                MimeType mime = MimeTable.getInstance().getContentTypeFor(documentUri);
                String contentType = null;
                if (mime != null) {
                    contentType = mime.getName();
                } else {
                    mime = MimeType.BINARY_TYPE;
                }

                try (Txn txn = pool.getTransactionManager().beginTransaction()) {

                    if (mime.isXMLType()) {
                        logger.debug("storing XML resource");

                        try (final FileInputSource source = new FileInputSource(tmpFile)) {

                            final IndexInfo info = collection.validateXMLResource(
                                txn, broker, documentUri, source
                            );

                            final DocumentImpl doc = info.getDocument();
                            doc.getMetadata().setMimeType(contentType);

                            collection.store(txn, broker, info, source, false);
                        }

                        logger.debug("done");

                    } else {
                        logger.debug("storing Binary resource");

                        try (InputStream is = Files.newInputStream(tmpFile)) {
                            collection.addBinaryResource(
                                txn, broker, documentUri, is, contentType, tmpFile.toFile().length()
                            );
                        }

                        logger.debug("done");
                    }

                    logger.debug("commit");

                    txn.success();
                }

            } finally {
                if (collection != null) {
                    collection.release(Lock.READ_LOCK);
                }
            }

        } catch (Exception e) {
            throw new IOException("fail to upload "+url, e);
        } finally {
            Files.deleteIfExists(tmpFile);
        }
    }

    @Override
    public void flush() throws IOException {
        bos.flush();
    }
}
