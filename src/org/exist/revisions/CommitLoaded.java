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

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static javax.xml.stream.XMLStreamReader.*;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class CommitLoaded implements CommitReader {

    String id;

    String author;
    String message;

    List<Change> acts = new ArrayList<>(256);

    CommitLoaded(RCSHolder holder, Path log) throws IOException, XMLStreamException {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.FALSE);

        try (Reader reader = Files.newBufferedReader(log)) {
            XMLStreamReader streamReader = factory.createXMLStreamReader(reader);

            while(streamReader.hasNext()){
                streamReader.next();

                switch (streamReader.getEventType()) {
                    case START_ELEMENT:

                        switch (streamReader.getLocalName()) {
                            case "RCS:commit-log":
                                id = streamReader.getAttributeValue("", "id");
                                break;

                            case "author":
                                author = streamReader.getElementText();
                                break;

                            case "message":
                                message = streamReader.getElementText();
                                break;

                            case "entry":
                                String id = streamReader.getAttributeValue("", "id");
                                String uri = streamReader.getAttributeValue("", "uri");
                                String path = streamReader.getAttributeValue("", "path");
                                String op = streamReader.getAttributeValue("", "operation");
                                String error = streamReader.getAttributeValue("", "error");

                                acts.add(new CommitAction(holder, op, uri, id, error, path));
                                break;
                        }
                        break;
                }
            }
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String author() {
        return author;
    }

    @Override
    public String message() {
        return message;
    }

    @Override
    public Iterable<Change> changes() {
        return acts;
    }
}
