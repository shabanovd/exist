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
package org.exist.xquery.backupsdiffs;

import org.exist.backup.restore.listener.RestoreListener;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import java.util.*;

public class XMLDiffsHandler extends DefaultHandler {

    DocumentNodeEntry doc;
    private Stack<Entry> stack = new Stack<>();

    private Map<Entry, List<Entry>> map = new HashMap<>();

    public XMLDiffsHandler() {
    }

    @Override
    public void startDocument() throws SAXException {
        DocumentNodeEntry node = doc = new DocumentNodeEntry();

        map.put(node, new ArrayList<>());
        stack.push(node);
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        super.processingInstruction(target, data);

        PiNodeEntry node = new PiNodeEntry(target+":"+data);

        map.put(node, new ArrayList<>());
    }

    /**
     * @see  org.xml.sax.ContentHandler#startElement(String, String, String, Attributes)
     */
    @Override
    public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {

        NodeEntry node = new NodeEntry(namespaceURI, localName, qName, atts);

        map.put(node, new ArrayList<>());
        stack.push(node);

        super.startElement(namespaceURI, localName, qName, atts);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);

        map.get(stack.peek()).add(new TextNodeEntry(new String(ch, start, length)));
    }

    @Override
    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {
        super.endElement(namespaceURI, localName, qName);

        stack.pop();
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();

        stack.pop();
    }

    public boolean diffs(XMLDiffsHandler o, RestoreListener l) {
        return doc.diffs(o, o.doc, l);
    }

    class Entry {

        String name;

        Attributes attributes;

        Entry(Attributes atts, String name) {
            attributes = new AttributesImpl(atts);
            this.name = name;
        }

        public String name() {
            return name;
        }

        public boolean diffs(XMLDiffsHandler scnd, Entry o, RestoreListener l) {

            boolean flag = true;

            //check attributes
            HashSet<String> set = new HashSet<>(attributes.getLength());

            for (int i = 0; i < attributes.getLength(); i++) {
                set.add(Utils.string(attributes, i));
            }

            for (int i = 0; i < o.attributes.getLength(); i++) {
                String str = Utils.string(o.attributes, i);

                if (!set.remove(str)) {
                    l.error("only 2nd "+o.name+" have attribute "+str);
                    flag = false;
                }
            }

            for (String str : set) {
                l.error("only 1st "+name+" have attribute "+str);
                flag = false;
            }

            //check children
            List<Entry> children = map.get(this);
            HashMap<String, List<Entry>> nodes = new HashMap<>(children.size());
            for (Entry child : children) {
                nodes.compute(child.toString(), (k,v) -> {
                    List<Entry> list;
                    if (v == null) {
                        list = new ArrayList<>();
                    } else {
                        list = v;
                    }
                    list.add(child);
                    return list;
                });
            }

            for (Entry child : scnd.map.get(o)) {
                List<Entry> list = nodes.get(child.toString());
                if (list == null || list.isEmpty()) {
                    l.error("only 2nd "+o.name+" have child "+child);
                    flag = false;
                } else {
                    list.remove(list.size() - 1);
                }
            }

            for (List<Entry> list : nodes.values()) {
                for (Entry child : list) {
                    l.error("only 1st "+name+" have child "+child);
                    flag = false;
                }
            }

            return flag;
        }
    }

    class NodeEntry extends Entry {

        String namespaceURI;
        String localName;
        String qName;

        NodeEntry(String namespaceURI, String localName, String qName, Attributes atts) {
            super(atts, ""+namespaceURI+":"+localName+":"+qName);

            this.namespaceURI = namespaceURI;
            this.localName = localName;
            this.qName = qName;
        }

        @Override
        public String toString() {
            ArrayList<String> list = new ArrayList<>(attributes.getLength());

            for (int i = 0; i < attributes.getLength(); i++) {
                list.add(Utils.string(attributes, i));
            }

            Collections.sort(list);

            StringBuilder sb = new StringBuilder();
            for (String str : list) {
                sb.append(str).append(",");
            }

            return ""+namespaceURI+":"+localName+":"+qName+"/"+sb.toString();
        }
    }

    class TextNodeEntry extends Entry {

        TextNodeEntry(String name) {
            super(new AttributesImpl(), name);
        }
    }

    class PiNodeEntry extends Entry {

        PiNodeEntry(String name) {
            super(new AttributesImpl(), name);
        }
    }

    class DocumentNodeEntry extends Entry {

        DocumentNodeEntry() {
            super(new AttributesImpl(), "");
        }
    }
}