/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2011-2012 The eXist Project
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

import org.apache.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.dom.DocumentImpl;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.Txn;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.ext.LexicalHandler;

/**
 *
 * @author aretter
 */
public class DocumentTriggersVisitor extends AbstractTriggersVisitor<DocumentTrigger> implements DocumentTrigger {

    protected final static Logger LOG = Logger.getLogger(DocumentTriggersVisitor.class);
    
    public DocumentTriggersVisitor(DocumentTriggerProxies proxies) {
        super(proxies);
    }
    
    public DocumentTriggersVisitor(List<DocumentTrigger> triggers) {
        super(triggers);
    }
    
    private void log(Exception e) {
        LOG.error(e.getMessage(), e);
    }
    
    @Override
    public void beforeCreateDocument(DBBroker broker, Txn txn, XmldbURI uri) throws TriggerException {
        for(final DocumentTrigger trigger : getTriggers()) {
            trigger.beforeCreateDocument(broker, txn, uri);
        }
    }

    @Override
    public void afterCreateDocument(DBBroker broker, Txn txn, DocumentImpl document) {
    	for(final DocumentTrigger trigger : getTriggers()) {
            try {
				trigger.afterCreateDocument(broker, txn, document);
			} catch (TriggerException e) { log(e); }
        }
    }

    @Override
    public void beforeUpdateDocument(DBBroker broker, Txn txn, DocumentImpl document) throws TriggerException {
        for(final DocumentTrigger trigger : getTriggers()) {
        	trigger.beforeUpdateDocument(broker, txn, document);
        }
    }

    @Override
    public void afterUpdateDocument(DBBroker broker, Txn txn, DocumentImpl document) {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
	        	trigger.afterUpdateDocument(broker, txn, document);
    		} catch (TriggerException e) { log(e); }
        }
    }

    @Override
    public void beforeCopyDocument(DBBroker broker, Txn txn, DocumentImpl document, XmldbURI newUri) throws TriggerException {
        for(final DocumentTrigger trigger : getTriggers()) {
            trigger.beforeCopyDocument(broker, txn, document, newUri);
        }
    }

    @Override
    public void afterCopyDocument(DBBroker broker, Txn txn, DocumentImpl document, XmldbURI oldUri) {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.afterCopyDocument(broker, txn, document, oldUri);
    		} catch (TriggerException e) { log(e); }
        }
    }

    @Override
    public void beforeMoveDocument(DBBroker broker, Txn txn, DocumentImpl document, XmldbURI newUri) throws TriggerException {
        for(final DocumentTrigger trigger : getTriggers()) {
            trigger.beforeMoveDocument(broker, txn, document, newUri);
        }
    }

    @Override
    public void afterMoveDocument(DBBroker broker, Txn txn, DocumentImpl document, XmldbURI oldUri) {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.afterMoveDocument(broker, txn, document, oldUri);
    		} catch (TriggerException e) { log(e); }
        }
    }

    @Override
    public void beforeDeleteDocument(DBBroker broker, Txn txn, DocumentImpl document) throws TriggerException {
        for(final DocumentTrigger trigger : getTriggers()) {
            trigger.beforeDeleteDocument(broker, txn, document);
        }
    }

    @Override
    public void afterDeleteDocument(DBBroker broker, Txn txn, XmldbURI uri) {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.afterDeleteDocument(broker, txn, uri);
    		} catch (TriggerException e) { log(e); }
        }
    }
    
    @Override
    public void startDocument() throws SAXException {
		for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.startDocument();
    		} catch (TriggerException e) { log(e); }
		}
    	
        if (outputHandler != null)
        	outputHandler.startDocument();
    }

    @Override
    public void endDocument() throws SAXException {
		for(final DocumentTrigger trigger : getTriggers()) {
			try {
				trigger.endDocument();
			} catch (TriggerException e) { log(e); }
		}
        
    	if (outputHandler != null)
        	outputHandler.endDocument();
    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
        	try {
        		trigger.startPrefixMapping(prefix, uri);
        	} catch (TriggerException e) { log(e); }
        }
        
        if (outputHandler != null)
        	outputHandler.startPrefixMapping(prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.endPrefixMapping(prefix);
			} catch (TriggerException e) { log(e); }
        }
        
        if (outputHandler != null)
        	outputHandler.endPrefixMapping(prefix);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.startElement(uri, localName, qName, atts);
			} catch (TriggerException e) { log(e); }
        }

        if (outputHandler != null)
        	outputHandler.startElement(uri, localName, qName, atts);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.endElement(uri, localName, qName);
			} catch (TriggerException e) { log(e); }
        }

        if (outputHandler != null)
        	outputHandler.endElement(uri, localName, qName);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.characters(ch, start, length);
			} catch (TriggerException e) { log(e); }
        }

        if (outputHandler != null)
        	outputHandler.characters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.ignorableWhitespace(ch, start, length);
			} catch (TriggerException e) { log(e); }
        }

        if (outputHandler != null)
        	outputHandler.ignorableWhitespace(ch, start, length);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.processingInstruction(target, data);
			} catch (TriggerException e) { log(e); }
        }

        if (outputHandler != null)
        	outputHandler.processingInstruction(target, data);
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.skippedEntity(name);
			} catch (TriggerException e) { log(e); }
        }

        if (outputHandler != null)
        	outputHandler.skippedEntity(name);
    }

    @Override
    public void startDTD(String name, String publicId, String systemId) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.startDTD(name, publicId, systemId);
			} catch (TriggerException e) { log(e); }
        }

        if (lexicalHandler != null)
        	lexicalHandler.startDTD(name, publicId, systemId);
    }

    @Override
    public void endDTD() throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.endDTD();
			} catch (TriggerException e) { log(e); }
        }
        
        if (lexicalHandler != null)
        	lexicalHandler.endDTD();
    }

    @Override
    public void startEntity(String name) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.startEntity(name);
			} catch (TriggerException e) { log(e); }
        }

        if (lexicalHandler != null)
        	lexicalHandler.startEntity(name);
    }

    @Override
    public void endEntity(String name) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.endEntity(name);
			} catch (TriggerException e) { log(e); }
        }

        if (lexicalHandler != null)
        	lexicalHandler.endEntity(name);
    }

    @Override
    public void startCDATA() throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.startCDATA();
			} catch (TriggerException e) { log(e); }
        }

        if (lexicalHandler != null)
        	lexicalHandler.startCDATA();
    }

    @Override
    public void endCDATA() throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.endCDATA();
			} catch (TriggerException e) { log(e); }
        }

        if (lexicalHandler != null)
        	lexicalHandler.endCDATA();
    }

    @Override
    public void comment(char[] ch, int start, int length) throws SAXException {
        for(final DocumentTrigger trigger : getTriggers()) {
            try {
            	trigger.comment(ch, start, length);
			} catch (TriggerException e) { log(e); }
        }

        if (lexicalHandler != null)
        	lexicalHandler.comment(ch, start, length);
    }
    
    @Override
    public void setDocumentLocator(Locator locator) {
        for(final DocumentTrigger trigger : getTriggers()) {
            trigger.setDocumentLocator(locator);
        }

        if (outputHandler != null)
        	outputHandler.setDocumentLocator(locator);
    }
    
    private boolean validating = true;
    
    @Override
    public boolean isValidating() {
        return this.validating;
    }

    @Override
    public void setValidating(boolean validating) {
        this.validating = validating;
        for(final DocumentTrigger trigger : getTriggers()) {
            trigger.setValidating(validating);
        }
    }

    private ContentHandler outputHandler;
    
    @Override
    public ContentHandler getOutputHandler() {
        return outputHandler;
    }
            
    @Override
    public void setOutputHandler(ContentHandler outputHandler) {
        this.outputHandler = outputHandler;
    }
    
    private LexicalHandler lexicalHandler;
    
    @Override
    public LexicalHandler getLexicalOutputHandler() {
        return lexicalHandler;
    }

    @Override
    public void setLexicalOutputHandler(LexicalHandler lexicalHandler) {
        this.lexicalHandler = lexicalHandler;
    }

    @Override
    public ContentHandler getInputHandler() {
        return outputHandler;
    }
    
    @Override
    public LexicalHandler getLexicalInputHandler() {
        return lexicalHandler;
    }

	@Override
	public void beforeUpdateDocumentMetadata(DBBroker broker, Txn txn, DocumentImpl document) throws TriggerException {
        for(final DocumentTrigger trigger : getTriggers()) {
        	trigger.beforeUpdateDocumentMetadata(broker, txn, document);
        }
	}

	@Override
	public void afterUpdateDocumentMetadata(DBBroker broker, Txn txn, DocumentImpl document) {
        for(final DocumentTrigger trigger : getTriggers()) {
        	try {
        		trigger.afterUpdateDocumentMetadata(broker, txn, document);
			} catch (TriggerException e) { log(e); }
        }
	}
}