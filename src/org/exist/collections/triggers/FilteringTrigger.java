/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2003-2012 The eXist Project
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
package org.exist.collections.triggers;

import java.util.List;
import java.util.Map;

import org.exist.collections.Collection;
import org.exist.storage.DBBroker;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;

/**
 * Abstract default implementation of a Trigger. This implementation just forwards
 * all SAX events to the output content handler.
 *  
 * @author wolf
 */
public abstract class FilteringTrigger implements DocumentTrigger {

    // The output handlers to which SAX events should be
    // forwarded
    private ContentHandler outputHandler = null;
    private LexicalHandler lexicalOutputHandler = null;
    private Collection collection = null;
    private boolean validating = true;

    protected Collection getCollection() {
        return collection;
    }

    /**
     * Configure the trigger. The default implementation just stores the parent collection
     * reference into the field {@link #collection collection}. Use method {@link #getCollection() getCollection}
     * to later retrieve the collection. 
     */
    @Override
    public void configure(DBBroker broker, Collection parent, Map<String, List<?>> parameters) throws TriggerException {
        this.collection = parent;
    }

    @Override
    public void setValidating(boolean validating) {
        this.validating = validating;
    }

    @Override
    public boolean isValidating() {
        return validating;
    }

    @Override
    public ContentHandler getInputHandler() {
        return this;
    }

    @Override
    public LexicalHandler getLexicalInputHandler() {
        return this;
    }

    @Override
    public ContentHandler getOutputHandler() {
        return outputHandler;
    }

    @Override
    public LexicalHandler getLexicalOutputHandler() {
        return lexicalOutputHandler;
    }

    @Override
    public void setOutputHandler(ContentHandler handler) {
        outputHandler = handler;
    }

    @Override
    public void setLexicalOutputHandler(LexicalHandler handler) {
        lexicalOutputHandler = handler;
    }

    @Override
    public void setDocumentLocator(Locator locator) {
    	if (outputHandler != null)
    		outputHandler.setDocumentLocator(locator);
    }

    @Override
    public void startDocument() throws SAXException {
    	if (outputHandler != null)
    		outputHandler.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
    	if (outputHandler != null)
    		outputHandler.endDocument();
    }

    @Override
    public void startPrefixMapping(String prefix, String namespaceURI) throws SAXException {
    	if (outputHandler != null)
    		outputHandler.startPrefixMapping(prefix, namespaceURI);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
    	if (outputHandler != null)
    		outputHandler.endPrefixMapping(prefix);
    }

    @Override
    public void startElement(String namespaceURI, String localName, String qname, Attributes attributes) throws SAXException {
    	if (outputHandler != null)
    		outputHandler.startElement(namespaceURI, localName, qname, attributes);
    }

    @Override
    public void endElement(String namespaceURI, String localName, String qname) throws SAXException {
    	if (outputHandler != null)
    		outputHandler.endElement(namespaceURI, localName, qname);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
    	if (outputHandler != null)
    		outputHandler.characters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
    	if (outputHandler != null)
    		outputHandler.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
    	if (outputHandler != null)
    		outputHandler.processingInstruction(target, data);
    }

    @Override
    public void skippedEntity(String arg0) throws SAXException {
    	if (outputHandler != null)
    		outputHandler.skippedEntity(arg0);
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
    	if (lexicalOutputHandler != null)
    		lexicalOutputHandler.startDTD(name, publicId, systemId);
    }

    @Override
    public void endDTD() throws SAXException {
    	if (lexicalOutputHandler != null)
    		lexicalOutputHandler.endDTD();
    }

    @Override
    public void startEntity(String arg0) throws SAXException {
    	if (lexicalOutputHandler != null)
    		lexicalOutputHandler.startEntity(arg0);
    }

    @Override
    public void endEntity(String arg0) throws SAXException {
    	if (lexicalOutputHandler != null)
    		lexicalOutputHandler.endEntity(arg0);
    }

    @Override
    public void startCDATA() throws SAXException {
    	if (lexicalOutputHandler != null)
    		lexicalOutputHandler.startCDATA();
    }

    @Override
    public void endCDATA() throws SAXException {
    	if (lexicalOutputHandler != null)
    		lexicalOutputHandler.endCDATA();
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
    	if (lexicalOutputHandler != null)
    		lexicalOutputHandler.comment(ch, start, length);
    }
}