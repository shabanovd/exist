/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2012-2014 The eXist Project
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
package org.exist.backup;

import org.exist.Resource;
import org.exist.collections.Collection;
import org.exist.util.serializer.SAXSerializer;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public interface BackupHandler {

    public void backup(Resource resource, XMLStreamWriter writer) throws IOException;

	public void backup(Collection collection, AttributesImpl attrs);
	public void backup(Collection collection, SAXSerializer serializer) throws SAXException;

	public void backup(Document document, AttributesImpl attrs);
	public void backup(Document document, SAXSerializer serializer) throws SAXException;
}
