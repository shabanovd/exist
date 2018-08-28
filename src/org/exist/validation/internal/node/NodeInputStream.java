/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2010 The eXist Project
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
 *  $Id$
 */

package org.exist.validation.internal.node;

import java.io.IOException;
import java.io.InputStream;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import javax.xml.transform.OutputKeys;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.storage.serializers.Serializer;
import org.exist.util.serializer.SAXSerializer;
import org.exist.util.serializer.SerializerPool;
import org.exist.xquery.value.NodeValue;

/**
 * @author Dannes Wessels (dizzzz@exist-db.org)
 */
public class NodeInputStream extends InputStream {

    private final static Logger logger = LogManager.getLogger(NodeInputStream.class);

    //parse serialization options
    private final static Properties outputProperties = new Properties();
    static {
        outputProperties.setProperty(OutputKeys.INDENT, "yes");
        outputProperties.setProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    }

    private Path tmpFile = null;
    private InputStream bis = null;

    /** Creates a new instance of NodeInputStream */
    public NodeInputStream(Serializer serializer, NodeValue node) throws IOException {
        logger.debug("Initializing NodeInputStream");

        tmpFile = Files.createTempFile("NodeInputStream-", ".tmp");
        try (OutputStream os = Files.newOutputStream(tmpFile)) {
            serialize(serializer, node, os);
        }

        if (tmpFile == null) throw new IOException("no data for "+node);

        bis = Files.newInputStream(tmpFile);
        
        logger.debug("Initializing NodeInputStream done");
    }

    private void serialize(Serializer serializer, NodeValue node, OutputStream os) throws IOException {

        logger.debug("Serializing started.");

        final SAXSerializer sax = (SAXSerializer) SerializerPool.getInstance().borrowObject(SAXSerializer.class);
        final String encoding = outputProperties.getProperty(OutputKeys.ENCODING, "UTF-8");
        try (Writer writer = new OutputStreamWriter(os, encoding)) {

            sax.setOutput(writer, outputProperties);

            serializer.reset();
            serializer.setProperties(outputProperties);
            serializer.setSAXHandlers(sax, sax);


            sax.startDocument();
            serializer.toSAX(node);

//            while(node.hasNext()) {
//                NodeValue next = (NodeValue)node.nextItem();
//                serializer.toSAX(next);
//            }

            sax.endDocument();

        } catch(final Exception e) {
            final String txt = "A problem occurred while serializing the node set";
            logger.debug(txt+".", e);
            throw new IOException(txt+": " + e.getMessage(), e);

        } finally {
            logger.debug("Serializing done.");
            SerializerPool.getInstance().returnObject(sax);
        }
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
    public int available() throws IOException {
        return bis.available();
    }

    @Override
    public void close() throws IOException {
        try {
            bis.close();
        } finally {
            Files.deleteIfExists(tmpFile);
        }
    }
}
