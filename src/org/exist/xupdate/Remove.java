/*
 * eXist Open Source Native XML Database Copyright (C) 2001-04 Wolfgang M. Meier
 * wolfgang@exist-db.org http://exist.sourceforge.net
 * 
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option) any
 * later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 * 
 * $Id$
 */
package org.exist.xupdate;

import java.util.Map;

import org.exist.EXistException;
import org.exist.dom.DocumentImpl;
import org.exist.dom.DocumentSet;
import org.exist.dom.NodeImpl;
import org.exist.dom.StoredNode;
import org.exist.security.Permission;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.DBBroker;
import org.exist.storage.NotificationService;
import org.exist.storage.UpdateListener;
import org.exist.storage.txn.Txn;
import org.exist.util.LockException;
import org.exist.xquery.XPathException;
import org.w3c.dom.Node;

/**
 * Implements an XUpdate remove operation.
 * 
 * @author Wolfgang Meier
 */
public class Remove extends Modification {

	/**
	 * Constructor for Remove.
	 * 
	 * @param pool
	 * @param user
	 * @param selectStmt
	 */
	public Remove(DBBroker broker, DocumentSet docs, String selectStmt,
			Map namespaces, Map variables) {
		super(broker, docs, selectStmt, namespaces, variables);
	}

	/**
	 * @see org.exist.xupdate.Modification#process(org.exist.dom.DocumentSet)
	 */
	public long process(Txn transaction) throws PermissionDeniedException,
			LockException, EXistException, XPathException {
		try {
			StoredNode[] ql = selectAndLock();
			IndexListener listener = new IndexListener(ql);
			NotificationService notifier = broker.getBrokerPool()
					.getNotificationService();
			NodeImpl parent;
			DocumentSet modifiedDocs = new DocumentSet();
			for (int i = 0; i < ql.length; i++) {
				StoredNode node = ql[i];
				DocumentImpl doc = (DocumentImpl)node.getOwnerDocument();
				if (!doc.getPermissions().validate(broker.getUser(),
						Permission.UPDATE))
					throw new PermissionDeniedException(
							"permission to update document denied");
				doc.getMetadata().setIndexListener(listener);
				modifiedDocs.add(doc);
				parent = (NodeImpl) node.getParentNode();				
				if (parent.getNodeType() != Node.ELEMENT_NODE) {
					LOG.debug("parent = " + parent.getNodeType() + "; "
							+ parent.getNodeName());
					throw new EXistException(
							"you cannot remove the document element. Use update "
									+ "instead");
				} else
					parent.removeChild(transaction, node);
				doc.getMetadata().clearIndexListener();
				doc.getMetadata().setLastModified(System.currentTimeMillis());
				broker.storeXMLResource(transaction, doc);
				notifier.notifyUpdate(doc, UpdateListener.UPDATE);
			}
			checkFragmentation(transaction, modifiedDocs);
			return ql.length;
		} finally {
			unlockDocuments();
		}
	}

	/**
	 * @see org.exist.xupdate.Modification#getName()
	 */
	public String getName() {
		return "remove";
	}

}