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
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist;

import org.exist.collections.Collection;
import org.exist.dom.DocumentImpl;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.Txn;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public interface TrashManager {

    /**
     * Method return collection where resource should be moved to instead of remove.
     *
     * To allow remove operation return null.
     *
     * @param broker
     * @param txn
     * @param col
     * @return
     */
    Collection move(DBBroker broker, Txn txn, Collection col);

    /**
     * Method return collection where resource should be moved to instead of remove.
     *
     * To allow remove operation return null.
     *
     * @param broker
     * @param txn
     * @param doc
     * @return
     */
    Collection move(DBBroker broker, Txn txn, DocumentImpl doc);

}
