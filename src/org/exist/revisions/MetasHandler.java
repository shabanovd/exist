/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2014 The eXist Project
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
package org.exist.revisions;

import org.exist.xmldb.XmldbURI;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.bind.DatatypeConverter;

/**
* @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
*/
class MetasHandler extends DefaultHandler {

    String uuid;
    String parentUuid;

    String type;

    XmldbURI uri;
    String name;
    String mimeType;

    long createdTime;
    long lastModified;

    //Permission perm;

    String content = null;

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);

        if (content == null) return;

        switch(qName){
        case Constants.EL_FILE_NAME:
            this.name = content;
            break;
        case Constants.EL_FILE_PATH:
            this.uri = XmldbURI.create(content);
            break;

        case Constants.EL_RESOURCE_TYPE:
            this.type = content;
            break;

        case Constants.EL_UUID:
            uuid = content;
            break;
        case Constants.PARENT_UUID:
            parentUuid = content;
            break;
        case Constants.EL_META_TYPE:
            mimeType = content;
            break;
        case Constants.EL_CREATED:
            createdTime = DatatypeConverter.parseDateTime(content).getTimeInMillis();
            break;
        case Constants.EL_LAST_MODIFIED:
            lastModified = DatatypeConverter.parseDateTime(content).getTimeInMillis();
            break;
        }

        content = null;
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        content = String.copyValueOf(ch, start, length).trim();

        super.characters(ch, start, length);
    }
}
