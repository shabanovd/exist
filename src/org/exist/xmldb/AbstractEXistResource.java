/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-06 Wolfgang M. Meier
 *  wolfgang@exist-db.org
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
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 * 
 *  $Id$
 */
package org.exist.xmldb;

import java.util.Date;

import org.exist.dom.DocumentImpl;
import org.exist.security.Permission;
import org.exist.security.PermissionDeniedException;
import org.exist.security.Subject;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.storage.lock.Lock.LockMode;
import org.exist.storage.txn.Txn;
import org.exist.util.LockException;
import org.exist.util.function.FunctionE;
import org.exist.xmldb.function.LocalXmldbDocumentFunction;
import org.w3c.dom.DocumentType;
import org.xml.sax.ext.LexicalHandler;
import org.xmldb.api.base.ErrorCodes;
import org.xmldb.api.base.XMLDBException;

/**
 * Abstract base implementation of interface EXistResource.
 */
public abstract class AbstractEXistResource extends AbstractLocal implements EXistResource {

	protected XmldbURI docId = null;
	protected String mimeType = null;
    protected boolean isNewResource = false;
    
	public AbstractEXistResource(Subject user, BrokerPool pool, LocalCollection parent, XmldbURI docId, String mimeType) {
		super(user, pool, parent);
		this.docId = docId.lastSegment();
        this.mimeType = mimeType;
	}
	
	/**
	 * 
	 * @param user
	 * @param pool
	 * @param parent
	 * @param docId
	 * @param mimeType
	 * 
	 * @deprecated Use the XmldbURI constructor instead
	 */
	public AbstractEXistResource(Subject user, BrokerPool pool, LocalCollection parent, String docId, String mimeType) {
		this(user, pool, parent, XmldbURI.create(docId), mimeType);
	}
	
	/* (non-Javadoc)
	 * @see org.exist.xmldb.EXistResource#getCreationTime()
	 */
	public abstract Date getCreationTime() throws XMLDBException;

	/* (non-Javadoc)
	 * @see org.exist.xmldb.EXistResource#getLastModificationTime()
	 */
	public abstract Date getLastModificationTime() throws XMLDBException;

	@Override
	public void setLastModificationTime(final Date lastModificationTime) throws XMLDBException {
		if(lastModificationTime.before(getCreationTime())) {
			throw new XMLDBException(ErrorCodes.PERMISSION_DENIED, "Modification time must be after creation time.");
		}

		modify((document, broker, transaction) -> {
			if (document == null) {
				throw new XMLDBException(ErrorCodes.INVALID_RESOURCE, "Resource " + docId + " not found");
			}

			document.getMetadata().setLastModified(lastModificationTime.getTime());
			return null;
		});
	}

	/* (non-Javadoc)
	 * @see org.exist.xmldb.EXistResource#getPermissions()
	 */
	public abstract Permission getPermissions() throws XMLDBException;
	
	/* (non-Javadoc)
	 * @see org.exist.xmldb.EXistResource#setLexicalHandler(org.xml.sax.ext.LexicalHandler)
	 */
	public void setLexicalHandler(LexicalHandler handler) {
	}
	
    public void setMimeType(String mime) {
        this.mimeType = mime;
    }
    
    public String getMimeType() throws XMLDBException {
        return mimeType;
    }
    
	protected DocumentImpl openDocument(DBBroker broker, LockMode lockMode) throws XMLDBException {
	    DocumentImpl document = null;
	    org.exist.collections.Collection parentCollection = null;
	    try {
	    	parentCollection = collection.getCollectionWithLock(lockMode);
		    if(parentCollection == null)
		    	{throw new XMLDBException(ErrorCodes.INVALID_COLLECTION, "Collection " + collection.getPath() + " not found");}
	        try {
	        	document = parentCollection.getDocumentWithLock(broker, docId, lockMode);
	        } catch (final PermissionDeniedException pde) {
                    throw new XMLDBException(ErrorCodes.PERMISSION_DENIED,
	        			"Permission denied for document " + docId + ": " + pde.getMessage());
                } catch (final LockException e) {
	        	throw new XMLDBException(ErrorCodes.PERMISSION_DENIED,
	        			"Failed to acquire lock on document " + docId);
	        }
		    if (document == null) {
		        throw new XMLDBException(ErrorCodes.INVALID_RESOURCE);
		    }
	//	    System.out.println("Opened document " + document.getName() + " mode = " + lockMode);
		    return document;
	    } finally {
	    	if(parentCollection != null)
	    		{parentCollection.release(lockMode);}
	    }
	}
	
	protected void closeDocument(DocumentImpl doc, LockMode lockMode) throws XMLDBException {
		if(doc == null)
			{return;}
//		System.out.println("Closed " + doc.getName() + " mode = " + lockMode);
		doc.getUpdateLock().release(lockMode);
	}

    public  DocumentType getDocType() throws XMLDBException {
    	return null;
        }

    public void setDocType(DocumentType doctype) throws XMLDBException {
		
    }

	/**
	 * Higher-order-function for performing read-only operations against this resource
	 *
	 * NOTE this read will occur using the database user set on the resource
	 *
	 * @param readOp The read-only operation to execute against the resource
	 * @return The result of the read-only operation
	 */
	protected <R> R read(final LocalXmldbDocumentFunction<R> readOp) throws XMLDBException {
		return withDb((broker, transaction) -> this.<R>read(broker, transaction).apply(readOp));
	}

	/**
	 * Higher-order-function for performing read-only operations against this resource
	 *
	 * @param broker The broker to use for the operation
	 * @param transaction The transaction to use for the operation
	 * @return A function to receive a read-only operation to perform against the resource
	 */
	public <R> FunctionE<LocalXmldbDocumentFunction<R>, R, XMLDBException> read(final DBBroker broker, final Txn transaction) throws XMLDBException {
		return with(LockMode.READ_LOCK, broker, transaction);
	}

	/**
	 * Higher-order-function for performing read/write operations against this resource
	 *
	 * NOTE this operation will occur using the database user set on the resource
	 *
	 * @param op The read/write operation to execute against the resource
	 * @return The result of the operation
	 */
	protected <R> R modify(final LocalXmldbDocumentFunction<R> op) throws XMLDBException {
		return withDb((broker, transaction) -> this.<R>modify(broker, transaction).apply(op));
	}

	/**
	 * Higher-order-function for performing read/write operations against this resource
	 *
	 * @param broker The broker to use for the operation
	 * @param transaction The transaction to use for the operation
	 * @return A function to receive an operation to perform against the resource
	 */
	public <R> FunctionE<LocalXmldbDocumentFunction<R>, R, XMLDBException> modify(final DBBroker broker, final Txn transaction) throws XMLDBException {
		return writeOp -> this.<R>with(LockMode.WRITE_LOCK, broker, transaction).apply((document, broker1, transaction1) -> {
			final R result = writeOp.apply(document, broker1, transaction1);
			broker.storeXMLResource(transaction1, document);
			return result;
		});
	}

	/**
	 * Higher-order function for performing lockable operations on this resource
	 *
	 * @param lockMode
	 * @param broker The broker to use for the operation
	 * @param transaction The transaction to use for the operation
	 * @return A function to receive an operation to perform on the locked database resource
	 */
	private <R> FunctionE<LocalXmldbDocumentFunction<R>, R, XMLDBException> with(final LockMode lockMode, final DBBroker broker, final Txn transaction) throws XMLDBException {
		return documentOp ->
			collection.<R>with(lockMode, broker, transaction).apply((collection, broker1, transaction1) -> {
				DocumentImpl doc = null;
				try {
					doc = collection.getDocumentWithLock(broker1, docId, lockMode);
					if(doc == null) {
						throw new XMLDBException(ErrorCodes.INVALID_RESOURCE);
					}
					return documentOp.apply(doc, broker1, transaction1);
				} finally {
					if(doc != null) {
						doc.getUpdateLock().release(lockMode);
					}
				}
			});
	}
}
