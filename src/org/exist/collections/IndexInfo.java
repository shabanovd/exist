/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2000-2013 The eXist Project
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
package org.exist.collections;

import org.exist.Indexer;
import org.exist.collections.triggers.DocumentTriggersVisitor;
import org.exist.dom.DocumentImpl;
import org.exist.security.Permission;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.Txn;
import org.exist.util.serializer.DOMStreamer;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.InputSource;

/**
 * Internal class used to track required fields between calls to
 * {@link org.exist.collections.Collection#validateXMLResource(Txn, DBBroker, XmldbURI, InputSource)} and
 * {@link org.exist.collections.Collection#store(Txn, DBBroker, IndexInfo, InputSource, boolean)}.
 * 
 * @author wolf
 */
public interface IndexInfo {

    public Indexer getIndexer();

    public void setTriggersVisitor(DocumentTriggersVisitor triggersVisitor);

    public DocumentTriggersVisitor getTriggersVisitor();

    public void setCreating(boolean creating);

    public boolean isCreating();
    
    public void setOldDocPermissions(final Permission oldDocPermissions);
    
    public Permission getOldDocPermissions();

    public DOMStreamer getDOMStreamer();

    public DocumentImpl getDocument();

    public CollectionConfiguration getCollectionConfig();
}
