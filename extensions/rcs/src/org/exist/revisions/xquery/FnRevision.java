/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2020 The eXist Project
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
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;
import org.xml.sax.SAXException;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class FnRevision extends BasicFunction {
    
    final static QName REV_AS_XML = new QName("revision-as-xml", Module.NAMESPACE_URI, Module.PREFIX);
    final static QName REV_AS_BIN = new QName("revision-as-binary", Module.NAMESPACE_URI, Module.PREFIX);
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            REV_AS_XML,
            "Get revision.",
            new SequenceType[]{
                new FunctionParameterSequenceType("oid", Type.STRING, Cardinality.EXACTLY_ONE,
                    "Organization id"),
                new FunctionParameterSequenceType("uuid", Type.STRING, Cardinality.EXACTLY_ONE,
                    "Resource UUID."),
                new FunctionParameterSequenceType("rev-id", Type.LONG, Cardinality.EXACTLY_ONE,
                    "Revision id")
            },
            new FunctionReturnSequenceType(Type.DOCUMENT, Cardinality.ZERO_OR_ONE, "")
        ),
        new FunctionSignature(
            REV_AS_BIN,
            "Get revision.",
            new SequenceType[]{
                new FunctionParameterSequenceType("oid", Type.STRING, Cardinality.EXACTLY_ONE,
                    "Organization id"),
                new FunctionParameterSequenceType("uuid", Type.STRING, Cardinality.EXACTLY_ONE,
                    "Resource UUID."),
                new FunctionParameterSequenceType("rev-id", Type.LONG, Cardinality.EXACTLY_ONE,
                    "Revision id")
            },
            new FunctionReturnSequenceType(Type.BASE64_BINARY, Cardinality.ZERO_OR_ONE, "")
        )
    };

    public FnRevision(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        String oid = args[0].getStringValue();
        RCSHolder holder = RCSManager.get().getHolder(oid);

        if (holder == null) throw new XPathException(this, "No organisation  '"+oid+"'.");
        
        String uuid = args[1].getStringValue();
        
        try {
            RCSResource resource = holder.resource(uuid);
            
            Revision rev = resource.revision(Long.valueOf( args[2].getStringValue() ));
            
            if (getSignature().getName() == REV_AS_BIN) {
                
                return rev.getBinaryValue(getContext());
                
            } else if (getSignature().getName() == REV_AS_XML) {
                
                return rev.getXML(getContext().getDatabase());
                
            } else {
                throw new XPathException(this, "Unknown signature '"+getSignature().getName()+"'.");
            }
            
        } catch (IOException | SAXException e) {
            throw new XPathException(this, e);
        }
    }
}
