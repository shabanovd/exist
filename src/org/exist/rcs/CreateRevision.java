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
package org.exist.rcs;

import java.nio.file.Path;

import org.exist.dom.QName;
import org.exist.memtree.MemTreeBuilder;
import org.exist.memtree.NodeImpl;
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
import org.xml.sax.helpers.AttributesImpl;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class CreateRevision extends BasicFunction {
    
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("commit", Module.NAMESPACE_URI, Module.PREFIX),
            "Create current document state revision.",
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

    public CreateRevision(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        
        RCSManager manager = RCSManager.get();
        
        ResponseBuilder rb = new ResponseBuilder();
        
        manager.commit(args[0].getStringValue(), args[1], rb);
        
        return rb.report();
    }
    
    class ResponseBuilder implements Handler {
        
        final MemTreeBuilder builder;
        
        final int nodeNr;
        
        AttributesImpl attribs;
        
        public ResponseBuilder() {

            builder = new MemTreeBuilder();
            builder.startDocument();
            
            // start root element
            nodeNr = builder.startElement("", "results", "results", null);
            
            builder.namespaceNode("exist", "http://exist.sourceforge.net/NS/exist");

            attribs = new AttributesImpl();
        }
        
        public NodeImpl report() {
            // finish root element
            builder.endElement();
            
            //System.out.println(builder.getDocument().toString());
            
            return builder.getDocument().getNode(nodeNr);
        }

        @Override
        public void processed(XmldbURI uri) {
            attribs.clear();
            
            attribs.addAttribute("", "uri", "uri", "CDATA", uri.toString());
            attribs.addAttribute("", "status", "status", "CDATA", "processed");
            
            builder.startElement("", "entry", "entry", attribs);
            builder.endElement();
        }

        @Override
        public void error(XmldbURI uri, Exception e) {
            attribs.clear();
            
            attribs.addAttribute("", "uri", "uri", "CDATA", uri.toString());
            attribs.addAttribute("", "status", "status", "CDATA", "exception");
            attribs.addAttribute("", "msg", "msg", "CDATA", e.getMessage());
            
            builder.startElement("", "entry", "entry", attribs);
            builder.endElement();
        }

        @Override
        public void error(XmldbURI uri, String msg) {
            attribs.clear();
            
            attribs.addAttribute("", "uri", "uri", "CDATA", uri.toString());
            attribs.addAttribute("", "status", "status", "CDATA", "error");
            attribs.addAttribute("", "msg", "msg", "CDATA", msg);
            
            builder.startElement("", "entry", "entry", attribs);
            builder.endElement();
        }

        @Override
        public void error(Path location, Exception e) {
            attribs.clear();
            
            attribs.addAttribute("", "location", "location", "CDATA", location.toString());
            attribs.addAttribute("", "status", "status", "CDATA", "error");
            attribs.addAttribute("", "msg", "msg", "CDATA", e.getMessage());
            
            builder.startElement("", "FS-entry", "FS-entry", attribs);
            builder.endElement();
        }
    }
}
