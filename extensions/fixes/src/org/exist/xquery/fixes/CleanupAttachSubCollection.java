/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2017 The eXist Project
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
package org.exist.xquery.fixes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.dom.QName;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.Txn;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.*;
import org.exist.xquery.functions.xmldb.XMLDBModule;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

public class CleanupAttachSubCollection extends BasicFunction {
    protected static final Logger logger = LogManager.getLogger(CleanupAttachSubCollection.class);
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("cleanup-attach-sub-collection", Module.NAMESPACE_URI, Module.PREFIX),
            "Attach the sub collection $name from the collection $collection-uri. " +
                XMLDBModule.COLLECTION_URI,
            new SequenceType[]{
                new FunctionParameterSequenceType("collection-uri", Type.STRING, Cardinality.EXACTLY_ONE, "The collection URI"),
                new FunctionParameterSequenceType("name", Type.STRING, Cardinality.EXACTLY_ONE, "The resource")},
            new SequenceType(Type.ITEM, Cardinality.EMPTY)
        )
    };

    public CleanupAttachSubCollection(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        if( !context.getSubject().hasDbaRole() )
            throw new XPathException( this,
                "Permission denied, calling user '" + context.getSubject().getName() + "' must be a DBA");

        final XmldbURI colURL = XmldbURI.create(args[0].itemAt(0).getStringValue());
        final XmldbURI name = XmldbURI.createInternal(args[1].itemAt(0).getStringValue());

        DBBroker broker = context.getBroker();

        try (Txn tx = broker.beginTx()) {

            Collection parent = broker.getCollection(colURL);
            if (parent == null) throw new XPathException(this, "parent collection not found");

            Collection child = broker.getCollection(colURL.append(name));
            if (child == null) throw new XPathException(this, "child collection not found");

            parent.addCollection(broker, child, false);

            broker.saveCollection(tx, parent);

            tx.success();

        } catch (Exception e) {
            throw new XPathException(this, e);
        }

        return Sequence.EMPTY_SEQUENCE;
    }
}
