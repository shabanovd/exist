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
package org.exist.storage.md.xquery;

import org.exist.dom.DocumentImpl;
import org.exist.dom.QName;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.DBBroker;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import java.util.ArrayList;
import java.util.List;

import static org.exist.storage.lock.Lock.LockMode.READ_LOCK;
import static org.exist.storage.md.MetaData.NAMESPACE_URI;
import static org.exist.storage.md.MetaData.PREFIX;

/**
 * 
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class CheckIndex extends BasicFunction {

    private static final QName NAME = new QName("check-index", NAMESPACE_URI, PREFIX);
    private static final String DESCRIPTION = "Check index for document.";
    private static final SequenceType RETURN = new SequenceType(Type.STRING, Cardinality.ZERO_OR_MORE);

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            NAME,
            DESCRIPTION,
            new SequenceType[] {
                 new FunctionParameterSequenceType("resource-url", Type.STRING, Cardinality.ONE_OR_MORE, "The resource's urls.")
            },
            RETURN
        )
    };

    /**
     * @param context
     */
    public CheckIndex(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        List<String> errors = new ArrayList<>();

        DBBroker broker = getContext().getBroker();

        Item next;
        for (final SequenceIterator i = args[0].unorderedIterator(); i.hasNext(); ) {
            next = i.nextItem();

            XmldbURI uri = XmldbURI.create(next.getStringValue());

            DocumentImpl doc = null;
            try {
                doc = broker.getXMLResource(uri, READ_LOCK);

                broker.checkIndex(doc, errors);

            } catch (PermissionDeniedException e) {
                throw new XPathException(this, e);
            } finally {
                if (doc != null) doc.getUpdateLock().release(READ_LOCK);
            }
        }

        if (errors.isEmpty()) return Sequence.EMPTY_SEQUENCE;

        ValueSequence res = new ValueSequence(errors.size());
        errors.forEach(error -> res.add(new StringValue(error)));

        return res;
    }
}
