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
package org.exist.xquery.fixes;

import org.apache.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.dom.DocumentImpl;
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

public class CleanupRemove extends BasicFunction {
    protected static final Logger logger = Logger.getLogger(CleanupRemove.class);
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("cleanup-remove", Module.NAMESPACE_URI, Module.PREFIX),
            "Removes the resource $resource from the collection $collection-uri. " +
                XMLDBModule.COLLECTION_URI,
            new SequenceType[]{
                new FunctionParameterSequenceType("collection-uri", Type.STRING, Cardinality.EXACTLY_ONE, "The collection URI"),
                new FunctionParameterSequenceType("resource", Type.STRING, Cardinality.EXACTLY_ONE, "The resource")},
            new SequenceType(Type.ITEM, Cardinality.EMPTY)
        )
    };

    public CleanupRemove(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        if( !context.getSubject().hasDbaRole() )
            throw new XPathException( this,
                "Permission denied, calling user '" + context.getSubject().getName() + "' must be a DBA");

        final XmldbURI colURL = XmldbURI.create(args[0].itemAt(0).getStringValue());
        final XmldbURI docURL = XmldbURI.createInternal(args[1].itemAt(0).getStringValue());

        DBBroker broker = context.getBroker();

        try (Txn tx = broker.beginTx()) {

            Collection col = broker.getCollection(colURL);

            if (col == null) throw new XPathException(this, "collection not found");

            DocumentImpl doc = col.getDocument(broker, docURL);

            if (doc == null) throw new XPathException(this, "document not found");

            col.removeResource(tx, broker, doc);

        } catch (Exception e) {
            throw new XPathException(this, e);
        }

        return Sequence.EMPTY_SEQUENCE;
    }
}
