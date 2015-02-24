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

import org.exist.dom.QName;
import org.exist.revisions.RCSHolder;
import org.exist.revisions.RCSManager;
import org.exist.revisions.RCSResource;
import org.exist.revisions.Revision;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.IntegerValue;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;
import org.exist.xquery.value.ValueSequence;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class FnRevisions extends BasicFunction {
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("revisions", Module.NAMESPACE_URI, Module.PREFIX),
            "Get revisions id for resource.",
            new SequenceType[]{
                new FunctionParameterSequenceType("oid", Type.STRING, Cardinality.EXACTLY_ONE,
                    "Organization id"),
                new FunctionParameterSequenceType("uuid", Type.STRING, Cardinality.EXACTLY_ONE,
                    "Resource UUID.")
            },
            new FunctionReturnSequenceType(Type.LONG, Cardinality.ZERO_OR_MORE, "")
        )
    };

    public FnRevisions(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        String oid = args[0].getStringValue();
        RCSHolder holder = RCSManager.get().getHolder(oid);

        if (holder == null) throw new XPathException(this, "No organisation  '"+oid+"'.");

        ValueSequence result = new ValueSequence();

        String uuid = args[1].getStringValue();
        
        try {
            RCSResource resource = holder.resource(uuid);
            
            for (Revision rev : resource.revisions()) {
                
                result.add( new IntegerValue(rev.id()));
            }
            
        } catch (IOException e) {
            throw new XPathException(this, e);
        }
        
        return result;
    }
}
