/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2014 The eXist Project
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
package org.exist.revisions.xquery;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.exist.collections.Collection;
import org.exist.dom.QName;
import org.exist.revisions.RCSHolder;
import org.exist.revisions.RCSManager;
import org.exist.security.PermissionDeniedException;
import org.exist.util.LockException;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class FnSnapshot extends BasicFunction {
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("snapshot", Module.NAMESPACE_URI, Module.PREFIX),
            "Create snapshot of collection, sub collections and documents.",
            new SequenceType[]{
                new FunctionParameterSequenceType("oid", Type.STRING, Cardinality.EXACTLY_ONE,
                        "Organization id"),
                new FunctionParameterSequenceType("collection", Type.STRING, Cardinality.EXACTLY_ONE,
                    "URI path of collections in database. Collection URIs should end on a '/'.")
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, "")
        )
    };

    public FnSnapshot(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        String oid = args[0].getStringValue();
        RCSHolder holder = RCSManager.get().getHolder(oid);

        if (holder == null) throw new XPathException(this, "No organisation  '"+oid+"'.");
        
        XmldbURI uri = XmldbURI.create(args[1].getStringValue());
        
        Collection collection;
        try {
            collection = getContext().getBroker().getCollection(uri);
        } catch (PermissionDeniedException e) {
            throw new XPathException(this, e);
        }
        
        ResponseBuilder rb = new ResponseBuilder();
        
        try {
            holder.snapshot(collection, rb);

        } catch (IOException | XMLStreamException | PermissionDeniedException | LockException e) {
            throw new XPathException(this, e);
        }
        
        return rb.report();
    }
}
