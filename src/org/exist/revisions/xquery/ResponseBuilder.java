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

import java.nio.file.Path;

import org.exist.dom.memtree.MemTreeBuilder;
import org.exist.dom.memtree.NodeImpl;
import org.exist.revisions.Handler;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.helpers.AttributesImpl;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class ResponseBuilder implements Handler {
    
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
    public void error(String id, String msg) {
        attribs.clear();

        attribs.addAttribute("", "resource-id", "resource-id", "CDATA", id);
        attribs.addAttribute("", "status", "status", "CDATA", "error");
        attribs.addAttribute("", "msg", "msg", "CDATA", msg);

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
