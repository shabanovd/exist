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

import org.exist.dom.QName;
import org.exist.revisions.RCSManager;
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
public class FnCommit extends BasicFunction {
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("commit", Module.NAMESPACE_URI, Module.PREFIX),
            "Create commit.",
            new SequenceType[]{
                new FunctionParameterSequenceType("msg", Type.STRING, Cardinality.EXACTLY_ONE,
                "Commit's log message"),
                new FunctionParameterSequenceType("paths", Type.STRING, Cardinality.ONE_OR_MORE,
                "URI paths of documents or collections in database. Collection URIs should end on a '/'.")
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, "")
        )
//        ,
//        new FunctionSignature(
//            new QName("revision-create", Module.NAMESPACE_URI, Module.PREFIX),
//            "Create current document state revision.",
//            new SequenceType[]{
//                new FunctionParameterSequenceType("paths", Type.STRING, Cardinality.ONE_OR_MORE,
//                "URI paths of documents or collections in database. Collection URIs should end on a '/'.")
//            },
//            new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, "")
//        ),
        
    };

    public FnCommit(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        
        RCSManager manager = RCSManager.get();
        
        ResponseBuilder rb = new ResponseBuilder();
        
        try {
            manager.commit(args[0].getStringValue(), args[1], rb);
        } catch (IOException | XMLStreamException e) {
            throw new XPathException(this, e);
        }
        
        return rb.report();
    }
}
