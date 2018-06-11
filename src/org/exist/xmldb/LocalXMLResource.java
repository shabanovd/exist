/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2015 The eXist Project
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
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 */
package org.exist.xmldb;

import com.evolvedbinary.j8fu.function.ConsumerE;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.exist.dom.persistent.NodeProxy;
import org.exist.dom.persistent.XMLUtil;
import org.exist.dom.memtree.AttrImpl;
import org.exist.dom.memtree.NodeImpl;
import org.exist.numbering.NodeId;
import org.exist.security.Subject;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.serializers.Serializer;
import org.exist.storage.txn.Txn;
import org.exist.util.MimeType;
import com.evolvedbinary.j8fu.Either;
import org.exist.util.serializer.DOMSerializer;
import org.exist.util.serializer.DOMStreamer;
import org.exist.util.serializer.SAXSerializer;
import org.exist.util.serializer.SerializerPool;
import org.exist.xquery.XPathException;
import org.exist.xquery.value.AtomicValue;
import org.exist.xquery.value.NodeValue;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.*;
import org.xml.sax.ext.LexicalHandler;
import org.xmldb.api.base.ErrorCodes;
import org.xmldb.api.base.XMLDBException;
import org.xmldb.api.modules.XMLResource;

import javax.annotation.Nullable;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Local implementation of XMLResource.
 */
public class LocalXMLResource extends AbstractEXistResource implements XMLResource {

    private NodeProxy proxy = null;

    private Properties outputProperties;
    private LexicalHandler lexicalHandler = null;

    // those are the different types of content this resource
    // may have to deal with
    protected String content = null;
    protected Path file = null;
    protected InputSource inputSource = null;
    protected Node root = null;
    protected AtomicValue value = null;

    public LocalXMLResource(final Subject user, final BrokerPool brokerPool, final LocalCollection parent, final XmldbURI did) throws XMLDBException {
        super(user, brokerPool, parent, did, MimeType.XML_TYPE.getName());
        this.outputProperties = parent != null ? parent.getProperties() : null;
    }

    public LocalXMLResource(final Subject user, final BrokerPool brokerPool, final LocalCollection parent, final NodeProxy p) throws XMLDBException {
        this(user, brokerPool, parent, p.getOwnerDocument().getFileURI());
        this.proxy = p;
        this.outputProperties = parent != null ? parent.getProperties() : null;
    }

    @Override
    public String getDocumentId() throws XMLDBException {
        return docId.toString();
    }

    @Override
    public String getResourceType() throws XMLDBException {
        return XMLResource.RESOURCE_TYPE;
    }

    @Override
    public Object getContent() throws XMLDBException {
        if (content != null) {            
            return content;
        }

        // Case 1: content is an external DOM node
        else if (root != null && !(root instanceof NodeValue)) {
            try(final StringWriter writer = new StringWriter()) {
                final DOMSerializer serializer = new DOMSerializer(writer, getProperties());
                try {
                    serializer.serialize(root);
                    content = writer.toString();
                } catch (final TransformerException e) {
                    throw new XMLDBException(ErrorCodes.INVALID_RESOURCE, e.getMessage(), e);
                }
                return content;
            } catch(final IOException e) {
                throw new XMLDBException(ErrorCodes.UNKNOWN_ERROR, e.getMessage(), e);
            }

        // Case 2: content is an atomic value
        } else if (value != null) {
            try {
                if (Type.subTypeOf(value.getType(),Type.STRING)) {
                    return ((StringValue)value).getStringValue(true);
                } else {
                    return value.getStringValue();
                }
            } catch (final XPathException e) {
                throw new XMLDBException(ErrorCodes.INVALID_RESOURCE, e.getMessage(), e);
            }

        // Case 3: content is a file
        } else if (file != null) {
            try {
                content = XMLUtil.readFile(file);
                return content;
            } catch (final IOException e) {
                throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "error while reading resource contents", e);
            }

        // Case 4: content is an input source
        } else if (inputSource != null) {
            try {
                content = XMLUtil.readFile(inputSource);
                return content;
            } catch (final IOException e) {
                throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "error while reading resource contents", e);
            }

        // Case 5: content is a document or internal node, we MUST serialize it
        } else {
            content = withDb((broker, transaction) -> {
                final Serializer serializer = broker.newSerializer();
                serializer.setUser(user);

                try {
                    serializer.setProperties(getProperties());

                    if (root != null) {
                        return serialize(broker, saxSerializer -> saxSerializer.toSAX((NodeValue) root));
                    } else if (proxy != null) {
                        return serialize(broker, saxSerializer -> saxSerializer.toSAX(proxy));
                    } else {
                        return this.<String>read(broker, transaction).apply((document, broker1, transaction1) -> {
                            try {
                                return serialize(broker, saxSerializer -> saxSerializer.toSAX(document));
                            } catch (final SAXException e) {
                                throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.getMessage(), e);
                            }
                        });
                    }
                } catch (final SAXException e) {
                    throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.getMessage(), e);
                }
            });
            return content;
        }
    }

    private String serialize(final DBBroker broker, final ConsumerE<Serializer, SAXException> toSaxFunction) throws SAXException, IOException {
        final Serializer serializer = broker.newSerializer();
        serializer.setUser(user);
        serializer.setProperties(getProperties());

        SAXSerializer saxSerializer = null;
        try {
            saxSerializer = (SAXSerializer) SerializerPool.getInstance().borrowObject(SAXSerializer.class);

            try (final StringWriter writer = new StringWriter()) {
                saxSerializer.setOutput(writer, getProperties());
                serializer.setSAXHandlers(saxSerializer, saxSerializer);

                toSaxFunction.accept(serializer);

                writer.flush();
                return writer.toString();
            }
        } finally {
            if (saxSerializer != null) {
                SerializerPool.getInstance().returnObject(saxSerializer);
            }
        }
    }

    @Override
    public Node getContentAsDOM() throws XMLDBException {
        final Node result;
        if (root != null) {
            if(root instanceof NodeImpl) {
                withDb((broker, transaction) -> {
                    ((NodeImpl)root).expand();
                    return null;
                });
            }
            result = root;
        } else if (value != null) {
            throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "cannot return an atomic value as DOM node");
        } else {
            result = read((document, broker, transaction) -> {
                if (proxy != null) {
                    return document.getNode(proxy);
                } else {
                    // <frederic.glorieux@ajlsm.com> return a full to get root PI and comments
                    return document;
                }
            });
        }

        return result;
    }

    @Override
    public void getContentAsSAX(final ContentHandler handler) throws XMLDBException {

        // case 1: content is an external DOM node
        if (root != null && !(root instanceof NodeValue)) {
            try {
                final String option = collection.getProperty(Serializer.GENERATE_DOC_EVENTS, "false");
                final DOMStreamer streamer = (DOMStreamer) SerializerPool.getInstance().borrowObject(DOMStreamer.class);
                streamer.setContentHandler(handler);
                streamer.setLexicalHandler(lexicalHandler);
                streamer.serialize(root, option.equalsIgnoreCase("true"));
                SerializerPool.getInstance().returnObject(streamer);
            } catch (final Exception e) {
                throw new XMLDBException(ErrorCodes.INVALID_RESOURCE, e.getMessage(), e);
            }
        } else {
            withDb((broker, transaction) -> {
                try {
                    // case 2: content is an atomic value
                    if (value != null) {
                        value.toSAX(broker, handler, getProperties());

                    // case 3: content is an internal node or a document
                    } else {
                        final Serializer serializer = broker.newSerializer();
                        serializer.setUser(user);
                        serializer.setProperties(getProperties());
                        serializer.setSAXHandlers(handler, lexicalHandler);
                        if (root != null) {
                            serializer.toSAX((NodeValue) root);

                        } else if (proxy != null) {
                            serializer.toSAX(proxy);

                        } else {
                            read(broker, transaction).apply((document, broker1, transaction1) -> {
                                try {
                                    serializer.toSAX(document);
                                    return null;
                                } catch(final SAXException e) {
                                    throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.getMessage(), e);
                                }
                            });
                        }
                    }
                    return null;
                } catch(final SAXException e) {
                    throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.getMessage(), e);
                }
            });
        }
    }

    /**
     * Sets the content for this resource. If value is of type File, it is
     * directly passed to the parser when Collection.storeResource is called.
     * Otherwise the method tries to convert the value to String.
     * 
     * Passing a File object should be preferred if the document is large. The
     * file's content will not be loaded into memory but directly passed to a
     * SAX parser.
     * 
     * @param obj the content value to set for the resource.
     * @exception XMLDBException with expected error codes. <br />
     *     <code>ErrorCodes.VENDOR_ERROR</code> for any vendor specific errors
     *     that occur. <br />
     */
    @Override
    public void setContent(final Object obj) throws XMLDBException {
        content = null;
        file = null;
        value = null;
        inputSource = null;
        root = null;

        if (obj instanceof Path) {
            file = (Path) obj;
        } else if (obj instanceof java.io.File) {
            file = ((java.io.File) obj).toPath();
        } else if (obj instanceof AtomicValue) {
            value = (AtomicValue) obj;
        } else if (obj instanceof InputSource) {
            inputSource=(InputSource) obj;
        } else if (obj instanceof byte[]) {
            content = new String((byte[])obj, UTF_8);
        } else {
            content = obj.toString();
        }
    }

    @Override
    public void setContentAsDOM(final Node root) throws XMLDBException {
        if (root instanceof AttrImpl) {
            throw new XMLDBException(ErrorCodes.WRONG_CONTENT_TYPE, "SENR0001: can not serialize a standalone attribute");
        }

        content = null;
        file = null;
        value = null;
        inputSource = null;
        this.root = root;
    }

    @Override
    public ContentHandler setContentAsSAX() throws XMLDBException {
        file = null;
        value = null;
        inputSource = null;
        root = null;
        return new InternalXMLSerializer();
    }

    @Override
    public boolean getSAXFeature(final String name) throws SAXNotRecognizedException, SAXNotSupportedException {
        return false;
    }

    @Override
    public void setSAXFeature(final String name, final boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
    }
	
    @Override
    public void setLexicalHandler(final LexicalHandler lexicalHandler) {
        this.lexicalHandler = lexicalHandler;
    }

    @Override
    public void setProperties(final Properties properties) {
        this.outputProperties = properties;
    }

    @Override
    @Nullable public Properties getProperties() {
        return outputProperties;
    }

    public NodeProxy getNode() throws XMLDBException {
        if(proxy != null) {
            return proxy;
        } else {
            return read((document, broker, transaction) -> new NodeProxy(document, NodeId.DOCUMENT_NODE));
        }
    }

    /**
     * Similar to {@link org.exist.xmldb.LocalXMLResource#getNode()}
     * but useful for operations within the XML:DB Local API
     * that are already working within a transaction
     */
    public NodeProxy getNode(final DBBroker broker, final Txn transaction) throws XMLDBException {
        if(proxy != null) {
            return proxy;
        } else {
            return this.<NodeProxy>read(broker, transaction).apply((document, broker1, transaction1) -> new NodeProxy(document, NodeId.DOCUMENT_NODE));
        }
    }

    @Override
	public  DocumentType getDocType() throws XMLDBException {
        return read((document, broker, transaction) -> document.getDoctype());
    }

    @Override
    public void setDocType(final DocumentType doctype) throws XMLDBException {
        modify((document, broker, transaction) -> {
            if (document == null) {
                throw new XMLDBException(ErrorCodes.INVALID_RESOURCE, "Resource " + docId + " not found");
            }

            document.setDocumentType(doctype);
            return null;
        });
    }
        
    private class InternalXMLSerializer extends SAXSerializer {
        public InternalXMLSerializer() {
            super(new StringWriter(), null);
        }

        @Override
        public void endDocument() throws SAXException {
            super.endDocument();
            content = getWriter().toString();
        }
    }
}
