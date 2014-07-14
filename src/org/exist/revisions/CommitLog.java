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

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class CommitLog implements AutoCloseable {

    enum Op {
        CREATE,
        UPDATE,
        MOVE,
        RENAME,
        DELETE
    }

    class Action {
        Op op;
        XmldbURI uri;
        String id;

        Action(Op op, XmldbURI uri, String id) {
            this.op = op;
            this.uri = uri;
            this.id = id;
        }

        Action(Op op, XmldbURI uri) {
            this.op = op;
            this.uri = uri;
            this.id = manager.uuid(uri, handler);
        }
    }

    RCSManager manager;
    Handler handler;

    boolean isDone = false;
    boolean isClosed = false;

    String id;

    String author;
    String message;

    List<Action> acts = new ArrayList<>(256);

    CommitLog(RCSManager manager, Handler handler) {
        this.handler = handler;
        this.manager = manager;
    }

    public String id() {
        return id;
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

        acts.add(new Action(Op.CREATE, uri));
        return this;
    }

    public CommitLog update(XmldbURI uri) {
        checkIsOpen();

        acts.add(new Action(Op.UPDATE, uri));
        return this;
    }

    public CommitLog move(XmldbURI uri) {
        checkIsOpen();

        acts.add(new Action(Op.MOVE, uri));
        return this;
    }

    public CommitLog rename(XmldbURI uri) {
        checkIsOpen();

        acts.add(new Action(Op.RENAME, uri));
        return this;
    }

    public CommitLog delete(String id, XmldbURI uri) {
        checkIsOpen();

        acts.add(new Action(Op.RENAME, uri, id));
        return this;
    }

    public void done() throws Exception {
        isDone = true;
    }

    @Override
    public void close() throws IOException, XMLStreamException {
        isClosed = true;

        if (isDone) manager.commit(this);
    }

    private void checkIsOpen() {
        if (isClosed) throw new RuntimeException("illegal commit use. it's closed.");
    }
}
