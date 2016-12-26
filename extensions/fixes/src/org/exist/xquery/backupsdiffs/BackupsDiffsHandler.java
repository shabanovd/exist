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

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.exist.Namespaces;
import org.exist.backup.BackupDescriptor;
import org.exist.backup.restore.listener.RestoreListener;
import org.exist.util.EXistInputSource;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public class BackupsDiffsHandler extends DefaultHandler {

    private final static Logger LOG = Logger.getLogger(BackupsDiffsHandler.class);
    private final static SAXParserFactory saxFactory = SAXParserFactory.newInstance();
    static {
        saxFactory.setNamespaceAware(true);
        saxFactory.setValidating(false);
    }

    private final RestoreListener listener;
    private final BackupDescriptor descriptor;

    ColEntry collection;
    Map<String, ColEntry> cols = new HashMap<>();
    Map<String, ResEntry> docs = new HashMap<>();

    //handler state
    private Stack<Entry> stack = new Stack<>();

    int level = 0;

    public BackupsDiffsHandler(RestoreListener listener, BackupDescriptor descriptor) {
        this.listener = listener;
        this.descriptor = descriptor;
    }

    @Override
    public void startDocument() throws SAXException {
        listener.setCurrentBackup(descriptor.getSymbolicPath());
    }

    /**
     * @see  org.xml.sax.ContentHandler#startElement(String, String, String, Attributes)
     */
    @Override
    public void startElement(String namespaceURI, String localName, String qName, Attributes atts) throws SAXException {

        level++;

        if (namespaceURI != null && namespaceURI.equals(Namespaces.EXIST_NS)) {
            if ("collection".equals(localName)) {
                stack.push(collectionEntry(atts));
                return;

            } else if ("resource".equals(localName)) {
                stack.push(resourceEntry(atts));
                return;

            } else if ("subcollection".equals(localName)) {
                ColEntry colEntry = restoreSubCollectionEntry(atts);
                cols.put(colEntry.name(), colEntry);
                return;

//            } else if ("deleted".equals(localName)) {
//                stack.peek().child(new NodeEntry(namespaceURI, localName, qName, atts));
            }
        }

        stack.peek().child(new NodeEntry(level, namespaceURI, localName, qName, atts));
    }

    @Override
    public void endElement(String namespaceURI, String localName, String qName) throws SAXException {

        if(namespaceURI != null && namespaceURI.equals(Namespaces.EXIST_NS)) {
            if ("collection".equals(localName)) {
                collection = (ColEntry) stack.pop();

            } if ("resource".equals(localName)) {
                ResEntry entry = (ResEntry) stack.pop();
                docs.put(entry.name(), entry);
            }
        }

        super.endElement(namespaceURI, localName, qName);

        level--;
    }

    private ColEntry restoreSubCollectionEntry(Attributes atts) throws SAXException {

        final String name;
        if(atts.getValue("filename") != null) {
            name = atts.getValue("filename");
        } else {
            name = atts.getValue("name");
        }

//        //exclude /db/system collection and sub-collections, as these have already been restored
////        if ((currentCollection.getURI().startsWith(XmldbURI.SYSTEM)))
////            return;
//
//        //parse the sub-collection descriptor and restore
//        final BackupDescriptor subDescriptor = descriptor.getChildBackupDescriptor(name);
//        if(subDescriptor != null) {
//
//            final SAXParser sax;
//            try {
//                sax = saxFactory.newSAXParser();
//
//                final XMLReader reader = sax.getXMLReader();
//
//                final EXistInputSource is = subDescriptor.getInputSource();
//                is.setEncoding( "UTF-8" );
//
//                final BackupsDiffsHandler handler = new BackupsDiffsHandler(broker, listener, subDescriptor, master);
//
//                reader.setContentHandler(handler);
//                reader.parse(is);
//            } catch(final SAXParseException e) {
//                throw new SAXException("Could not process collection: " + descriptor.getSymbolicPath(name, false), e);
//            } catch(final ParserConfigurationException pce) {
//                throw new SAXException("Could not initalise SAXParser for processing sub-collection: " + descriptor.getSymbolicPath(name, false), pce);
//            } catch(final IOException ioe) {
//                throw new SAXException("Could not read sub-collection for processing: " + ioe.getMessage(), ioe);
//            }
//        } else {
//            listener.error("Collection " + descriptor.getSymbolicPath(name, false) + " does not exist or is not readable.");
//        }

        return new ColEntry(atts, name);
    }

    private ColEntry collectionEntry(Attributes atts) throws SAXException {
        String name = atts.getValue("name");

        if (name == null) {
            throw new SAXException("Collection requires a name attribute");
        }

        return new ColEntry(atts, name);
    }



    private ResEntry resourceEntry(Attributes atts) throws SAXException {

        String name = atts.getValue("name");
        if (name == null) {
            throw new SAXException("Resource requires a name attribute");
        }

        String type;
        if (atts.getValue("type") != null) {
            type = atts.getValue("type");
        } else {
            type = "XMLResource";
        }

        String filename;
        if (atts.getValue("filename") != null) {
            filename = atts.getValue("filename");
        } else  {
            filename = name;
        }

        return new ResEntry(atts, name, filename, type);
    }

    public boolean diffs(Tasks tasks, BackupsDiffsHandler o) {

        System.out.println("col "+collection.name);

        //check collection
        boolean flag = collection.diffs(o.collection, listener);

        //check docs
        Map<String, ResEntry> setDocs = new HashMap<>(docs);
        for (Map.Entry<String, ResEntry> doc : o.docs.entrySet()) {

            ResEntry entry = setDocs.remove(doc.getKey());
            if (entry == null) {
                listener.error("only 2nd have "+o.collection.name+" / "+doc.getValue().name);
                flag = false;
            } else {
                flag = flag && entry.diffs(tasks, listener, doc.getValue(), descriptor, o.descriptor);
            }
        }

        for (ResEntry entry : setDocs.values()) {
            listener.error("only 1st have "+o.collection.name+" / "+entry.name);
            flag = false;
        }

        //check sub collections
        Map<String, ColEntry> setCols = new HashMap<>(cols);
        for (Map.Entry<String, ColEntry> col : o.cols.entrySet()) {

            ColEntry entry = setCols.remove(col.getKey());
            if (entry == null) {
                listener.error("only 2nd have "+o.collection.name);
                flag = false;
            } else {
                ColEntry oCol = col.getValue();

                flag = flag && entry.diffs(oCol, listener);
                tasks.submit(entry.task(tasks, listener, oCol, descriptor, o.descriptor));
            }
        }

        for (ColEntry entry : setCols.values()) {
            listener.error("only 1st have "+entry.name);
            flag = false;
        }

        return flag;
    }

    static class Entry {

        String name;

        Attributes attributes;

        List<NodeEntry> children = new ArrayList<>();

        Entry(Attributes atts, String name) {
            attributes = new AttributesImpl(atts);
            this.name = name;
        }

        public String name() {
            return name;
        }

        public void child(NodeEntry node) {
            children.add(node);
        }

        public boolean diffs(Entry o, RestoreListener l) {

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
            HashMap<String, List<NodeEntry>> nodes = new HashMap<>(children.size());
            for (NodeEntry child : children) {
                nodes.compute(child.toString(), (k,v) -> {
                    List<NodeEntry> list;
                    if (v == null) {
                        list = new ArrayList<>();
                    } else {
                        list = v;
                    }
                    list.add(child);
                    return list;
                });
            }

            for (NodeEntry child : o.children) {
                List<NodeEntry> list = nodes.get(child.toString());
                if (list == null || list.isEmpty()) {
                    l.error("only 2nd "+o.name+" have child "+child);
                    flag = false;
                } else {
                    list.remove(list.size() - 1);
                }
            }

            for (List<NodeEntry> children : nodes.values()) {
                for (NodeEntry child : children) {
                    l.error("only 1st "+name+" have child "+child);
                    flag = false;
                }
            }

            return flag;
        }
    }

    static class NodeEntry extends Entry {

        int level;
        String namespaceURI;
        String localName;
        String qName;

        NodeEntry(int level, String namespaceURI, String localName, String qName, Attributes atts) {
            super(atts, ""+namespaceURI+":"+localName+":"+qName);

            this.level = level;
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

            return ""+level+":"+namespaceURI+":"+localName+":"+qName+"/"+sb.toString();
        }
    }

    static class ColEntry extends Entry {

        ColEntry(Attributes atts, String name) {
            super(atts, name);
        }

        public Callable<Boolean> task(Tasks tasks, RestoreListener l, ColEntry o, BackupDescriptor dOne, BackupDescriptor dTwo) {
            return (Callable<Boolean>) () -> {
                final SAXParserFactory saxFactory = SAXParserFactory.newInstance();
                saxFactory.setNamespaceAware(true);
                saxFactory.setValidating(false);
                final SAXParser sax = saxFactory.newSAXParser();
                final XMLReader reader = sax.getXMLReader();

                final BackupDescriptor subOne = dOne.getChildBackupDescriptor(name);
                if (subOne == null) {
                    l.error("Collection " + dOne.getSymbolicPath(name, false) + " does not exist or is not readable.");
                    return false;
                }
                EXistInputSource isOne = subOne.getInputSource();
                isOne.setEncoding( "UTF-8" );

                BackupsDiffsHandler hOne = new BackupsDiffsHandler(l, subOne);

                reader.setContentHandler(hOne);
                reader.parse(isOne);

                final BackupDescriptor subTwo = dTwo.getChildBackupDescriptor(o.name);
                if (subTwo == null) {
                    l.error("Collection " + dTwo.getSymbolicPath(name, false) + " does not exist or is not readable.");
                    return false;
                }
                EXistInputSource isTwo = subTwo.getInputSource();
                isTwo.setEncoding( "UTF-8" );

                BackupsDiffsHandler hTwo = new BackupsDiffsHandler(l, subTwo);

                reader.setContentHandler(hTwo);
                reader.parse(isTwo);

                return hOne.diffs(tasks, hTwo);
            };
        }
    }

    static class ResEntry extends Entry {

        String filename;
        String type;

        ResEntry(Attributes atts, String name, String filename, String type) {
            super(atts, name);

            this.filename = filename;
            this.type = type;
        }

        public boolean diffs(Tasks tasks, RestoreListener l, ResEntry o, BackupDescriptor dOne, BackupDescriptor dTwo) {
            boolean flag = super.diffs(o, l);

            if ("XMLResource".equals(type)) {
                tasks.submit(new CheckXML(l, this, o, dOne, dTwo));
            } else {
                tasks.submit(new CheckBIN(l, this, o, dOne, dTwo));
            }

            return flag;
        }
    }

    static class CheckXML implements Callable<Boolean> {
        RestoreListener l;

        BackupDescriptor dOne;
        BackupDescriptor dTwo;

        ResEntry resOne;
        ResEntry resTwo;

        public CheckXML(RestoreListener l, ResEntry resOne, ResEntry resTwo, BackupDescriptor dOne, BackupDescriptor dTwo) {
            this.l = l;

            this.dOne = dOne;
            this.dTwo = dTwo;

            this.resOne = resOne;
            this.resTwo = resTwo;
        }

        @Override
        public Boolean call() throws Exception {

            EXistInputSource isOne = dOne.getInputSource(resOne.filename);
            if (isOne == null) {
                final String msg = "Failed to restore resource '" + resOne.name + "'\nfrom file '" + pathOne() + "'.\nReason: Unable to obtain its EXistInputSource";
                l.error(msg);
                return false;
            }

            EXistInputSource isTwo = dTwo.getInputSource(resTwo.filename);
            if (isTwo == null) {
                final String msg = "Failed to restore resource '" + resTwo.name + "'\nfrom file '" + pathTwo() + "'.\nReason: Unable to obtain its EXistInputSource";
                l.error(msg);
                return false;
            }

            final SAXParserFactory saxFactory = SAXParserFactory.newInstance();
            saxFactory.setNamespaceAware(true);
            saxFactory.setValidating(false);
            final SAXParser sax = saxFactory.newSAXParser();
            final XMLReader reader = sax.getXMLReader();


            isOne.setEncoding("UTF-8");

            XMLDiffsHandler hOne = new XMLDiffsHandler();

            reader.setContentHandler(hOne);
            reader.parse(isOne);

            isTwo.setEncoding("UTF-8");

            XMLDiffsHandler hTwo = new XMLDiffsHandler();

            reader.setContentHandler(hTwo);
            reader.parse(isTwo);

            boolean flag = hOne.diffs(hTwo, l);

            System.out.println(resOne.name+" "+flag+" "+pathOne()+" vs "+pathTwo());

            return flag;
        }

        private String pathOne() {
            return dOne.getSymbolicPath( resOne.filename, false );
        }

        private String pathTwo() {
            return dTwo.getSymbolicPath( resTwo.filename, false );
        }
    }

    static class CheckBIN implements Callable<Boolean> {
        RestoreListener l;

        BackupDescriptor dOne;
        BackupDescriptor dTwo;

        ResEntry resOne;
        ResEntry resTwo;

        public CheckBIN(RestoreListener l, ResEntry resOne, ResEntry resTwo, BackupDescriptor dOne, BackupDescriptor dTwo) {
            this.l = l;

            this.dOne = dOne;
            this.dTwo = dTwo;

            this.resOne = resOne;
            this.resTwo = resTwo;
        }

        @Override
        public Boolean call() throws Exception {

            EXistInputSource isOne = dOne.getInputSource(resOne.filename);
            if (isOne == null) {
                final String msg = "Failed to restore resource '" + resOne.name + "'\nfrom file '" + pathOne() + "'.\nReason: Unable to obtain its EXistInputSource";
                l.error(msg);
                return false;
            }

            EXistInputSource isTwo = dTwo.getInputSource(resTwo.filename);
            if (isTwo == null) {
                final String msg = "Failed to restore resource '" + resTwo.name + "'\nfrom file '" + pathTwo() + "'.\nReason: Unable to obtain its EXistInputSource";
                l.error(msg);
                return false;
            }

            try (
                    InputStream one = isOne.getByteStream();
                    InputStream two = isTwo.getByteStream();
            ) {
                boolean flag = IOUtils.contentEquals(one, two);

                System.out.println(resOne.name+" "+flag+" "+pathOne()+" vs "+pathTwo());

                if (!flag) {
                    l.error("different "+pathOne()+" vs "+pathTwo());
                    return false;
                }
            }
            return true;
        }

        private String pathOne() {
            return dOne.getSymbolicPath( resOne.filename, false );
        }

        private String pathTwo() {
            return dTwo.getSymbolicPath( resTwo.filename, false );
        }
    }
}