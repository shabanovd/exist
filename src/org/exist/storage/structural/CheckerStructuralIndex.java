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
package org.exist.storage.structural;

import com.google.common.primitives.SignedBytes;
import com.google.common.primitives.UnsignedBytes;
import org.exist.dom.*;
import org.exist.indexing.IndexWorker;
import org.exist.indexing.StreamListener;
import org.exist.numbering.NodeId;
import org.exist.storage.NodePath;
import org.exist.storage.RangeIndexSpec;
import org.exist.storage.btree.BTreeCallback;
import org.exist.storage.btree.BTreeException;
import org.exist.storage.btree.Value;
import org.exist.storage.txn.Txn;
import org.exist.xquery.TerminatedException;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.w3c.dom.Node;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class CheckerStructuralIndex implements AutoCloseable {

    NativeStructuralIndexWorker worker;

    Stream stream;

    DB db;

    ConcurrentMap<byte[], Long> map;
    AtomicInteger counter = new AtomicInteger();

    public CheckerStructuralIndex(NativeStructuralIndexWorker worker) {
        this.worker = worker;

        db = DBMaker.newTempFileDB()
                .deleteFilesAfterClose()
                .make();

        map = db.createTreeMap("structural")
                .counterEnable()
                //.keySerializer(BTreeKeySerializer.STRING)
                .valueSerializer(Serializer.LONG)
                .comparator(UnsignedBytes.lexicographicalComparator())
                .makeOrGet();

        stream = new Stream();
    }

    public Stream stream() {
        return stream;
    }

    private void addNode(QName qname, NodeProxy proxy) {
        int docId = proxy.getDocument().getDocId();
        byte type = qname.getNameType();

        byte[] key = worker.computeKey(type, qname, docId, proxy.getNodeId());

        map.put(key, worker.computeValue(proxy));

        key = worker.computeDocKey(type, docId, qname);
        map.putIfAbsent(key, 0L);

        if (counter.incrementAndGet() % 100000 == 0) {
            counter.set(0);
            db.commit();
        }
    }

    public void runChecker(Consumer<String> onError) throws Exception {
        db.commit();

        Iterator<Map.Entry<byte[], Long>> it = map.entrySet().iterator();

        AtomicLong count = new AtomicLong();

        worker.index.btree.query(null, (v, p) -> {
            if (!map.containsKey(v.getData())) {
                onError.accept("key must not exist " + Arrays.toString(v.getData()));
            }

            if (it.hasNext()) {
                Map.Entry<byte[], Long> e = it.next();
                if (!Arrays.equals(e.getKey(), v.getData())) {
                    onError.accept("wrong order of key " + Arrays.toString(v.getData()) + " vs " + Arrays.toString(e.getKey()));
                }

            } else {
                onError.accept("key must not exist " + Arrays.toString(v.getData()));
            }

            count.incrementAndGet();

            return true;
        });

        System.out.println(count);

        while (it.hasNext()) {
            onError.accept("index missing " + Arrays.toString(it.next().getKey()));
        }
    }

    @Override
    public void close() throws Exception {
        db.close();
    }

    class Stream implements StreamListener {

        @Override
        public IndexWorker getWorker() {
            return null;
        }

        @Override
        public void setNextInChain(StreamListener listener) {

        }

        @Override
        public StreamListener getNextInChain() {
            return null;
        }

        @Override
        public void startElement(Txn tx, ElementImpl element, NodePath path) {
            //super.startElement(tx, element, path);

            short indexType = RangeIndexSpec.NO_INDEX;
            if (element.getIndexType() != RangeIndexSpec.NO_INDEX)
                indexType = (short) element.getIndexType();

            NodeProxy proxy = new NodeProxy(document, element.getNodeId(), Node.ELEMENT_NODE, element.getInternalAddress());
            proxy.setIndexType(indexType);

            addNode(element.getQName(), proxy);
        }

        @Override
        public void attribute(Txn tx, AttrImpl attr, NodePath path) {
            //super.attribute(tx, attr, path);

            short indexType = RangeIndexSpec.NO_INDEX;
            if (attr.getIndexType() != RangeIndexSpec.NO_INDEX)
                indexType = (short) attr.getIndexType();

            NodeProxy proxy = new NodeProxy(document, attr.getNodeId(), Node.ATTRIBUTE_NODE, attr.getInternalAddress());
            proxy.setIndexType(indexType);
            addNode(attr.getQName(), proxy);
        }

        @Override
        public void characters(Txn tx, CharacterDataImpl text, NodePath path) {

        }

        @Override
        public void endElement(Txn tx, ElementImpl element, NodePath path) {

        }

        @Override
        public void metadata(Txn tx, String key, String value) {

        }

        DocumentImpl document = null;
        @Override
        public void startProcessing(Txn tx, DocumentImpl doc) {
            document = doc;
        }

        @Override
        public void endProcessing(Txn tx, DocumentImpl doc) {
            document = null;
        }
    }
}
