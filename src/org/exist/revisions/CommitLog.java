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

import org.exist.Operation;
import org.exist.xmldb.XmldbURI;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.exist.Operation.*;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class CommitLog implements CommitWriter, CommitReader {

    static final XmldbURI UNKNOWN_URI = XmldbURI.create("");

    class Action implements Change {
        protected Operation op;
        XmldbURI uri;
        String id;

        Action(Operation op, XmldbURI uri, String id) {
            this.op = op;
            this.uri = uri;
            this.id = id;
        }

        Action(Operation op, XmldbURI uri) {
            this.op = op;
            this.uri = uri;
            this.id = holder.uuid(uri, handler);
        }

        Action(Operation op, String id) {
            this.op = op;
            this.uri = UNKNOWN_URI;
            this.id = id;
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
            if (!(this.uri == null || this.uri == UNKNOWN_URI)) {
                throw new IllegalAccessError("uri already set");
            }

            this.uri = uri;
        }

        @Override
        public String error() {
            throw new IllegalAccessError("not allow");
        }

        @Override
        public Revision revision() {
            throw new IllegalAccessError("not allow");
        }
    }

    RCSHolder holder;
    Handler handler;

    boolean isDone = false;
    boolean isClosed = false;

    String id;

    String author;
    String message;

    List<Change> acts = new ArrayList<>(256);

    CommitLog(RCSHolder holder, Handler handler) {
        this.handler = handler;
        this.holder = holder;
    }

    public String id() {
        return id;
    }

    public RCSHolder rcsHolder() {
        return holder;
    }

    public CommitLog author(String author) {
        this.author = author;
        return this;
    }

    public String author() {
        return author;
    }

    public CommitLog message(String message) {
        this.message = message;
        return this;
    }

    public String message() {
        return message;
    }

    public CommitLog create(XmldbURI uri) {
        checkIsOpen();

        acts.add(new Action(CREATE, uri));
        return this;
    }

    public CommitLog create(String id) {
        checkIsOpen();

        acts.add(new Action(CREATE, id));
        return this;
    }

    public CommitLog update(XmldbURI uri) {
        checkIsOpen();

        acts.add(new Action(UPDATE, uri));
        return this;
    }

    public CommitLog update(String id) {
        checkIsOpen();

        acts.add(new Action(UPDATE, id));
        return this;
    }

    public CommitLog move(XmldbURI uri) {
        checkIsOpen();

        acts.add(new Action(MOVE, uri));
        return this;
    }

    public CommitLog move(String id) {
        checkIsOpen();

        acts.add(new Action(MOVE, id));
        return this;
    }

    public CommitLog rename(XmldbURI uri) {
        checkIsOpen();

        acts.add(new Action(RENAME, uri));
        return this;
    }

    public CommitLog rename(String id) {
        checkIsOpen();

        acts.add(new Action(RENAME, id));
        return this;
    }

    public CommitLog delete(String id, XmldbURI uri) {
        checkIsOpen();

        acts.add(new Action(DELETE, uri, id));
        return this;
    }

    public CommitLog delete(String id) {
        checkIsOpen();

        acts.add(new Action(DELETE, id));
        return this;
    }

    public void done() {
        isDone = true;
    }

    @Override
    public void close() throws IOException, XMLStreamException {
        if (isClosed) return;
        isClosed = true;

        if (isDone) holder.commit(this);
        else holder.rollback(this);
    }

    private void checkIsOpen() {
        if (isClosed) throw new RuntimeException("illegal commit use. it's closed.");
    }

    public Iterable<Change> changes() {
        return acts;
    }
}
