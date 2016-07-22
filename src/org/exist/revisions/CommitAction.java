/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2016 The eXist Project
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

import java.nio.file.Path;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class CommitAction implements Change {

    RCSHolder holder;

    Operation op;
    XmldbURI uri;
    String id;
    String error;
    String logPath;

    CommitAction(RCSHolder holder, String op, String uri, String id, String error, String logPath) {
        this.holder = holder;

        this.op = op == null ? null : Operation.valueOf(op);
        this.uri = XmldbURI.create(uri);
        this.id = id;
        this.error = error;
        this.logPath = logPath;
    }

    public String id() {
        return id;
    }

    public Operation operation() {
        return op;
    }

    public XmldbURI uri() {
        return uri;
    }

    public void uri(XmldbURI uri) {
        throw new IllegalAccessError("not allowed");
    }

    public String error() {
        return error;
    }

    public Revision revision() {
        Path location = holder.rcFolder.resolve(logPath);
        RCSResource resource = new RCSResource(holder, location.getParent());
        return new Revision(resource, location);
    }
}
