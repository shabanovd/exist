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

import com.google.common.primitives.UnsignedBytes;
import org.exist.dom.*;
import org.exist.indexing.IndexWorker;
import org.exist.indexing.StreamListener;
import org.exist.storage.NodePath;
import org.exist.storage.RangeIndexSpec;
import org.exist.storage.txn.Txn;
import org.exist.util.ByteConversion;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.w3c.dom.Node;

import javax.xml.ws.Holder;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
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
    AtomicLong counter = new AtomicLong();
    long ts = 0;

    public CheckerStructuralIndex(NativeStructuralIndexWorker worker, Path data) {
        this.worker = worker;

        if (data == null) {
            db = DBMaker.newTempFileDB()
                .deleteFilesAfterClose()
                .asyncWriteEnable()
                .transactionDisable()
                .make();
        } else {
            db = DBMaker.newFileDB(data.toFile())
                .asyncWriteEnable()
                .transactionDisable()
                .make();
        }

        map = db.createTreeMap("structural")
                .counterEnable()
                //.keySerializer(BTreeKeySerializer.STRING)
                .valueSerializer(Serializer.LONG)
                .comparator(UnsignedBytes.lexicographicalComparator())
                .makeOrGet();

        stream = new Stream();

        ts = System.currentTimeMillis();
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

        if (counter.incrementAndGet() % 10000000 == 0) {
            System.out.println("commit "+(System.currentTimeMillis() - ts));
            ts = System.currentTimeMillis();
            counter.set(0);
            db.commit();
        }
    }

    public Set<Integer> runChecker(Consumer<String> onError) throws Exception {
        db.commit();

        System.out.println("runChecker");

//        Iterator<Map.Entry<byte[], Long>> it = map.entrySet().iterator();

        System.out.println("total "+counter);
        counter.set(0);
        Holder<Integer> doc = new Holder<>(-1);

        Set<Integer> docsTotal = new HashSet<>();
        Set<Integer> docsWithErrors = new HashSet<>();

        try {
            worker.index.btree.query(null, (v, p) -> {

                byte[] k = v.getData();

                int docId = worker.readDocId(k);

                if (doc.value != docId) {
                    doc.value = docId;
                    docsTotal.add(docId);
                }

                if (!map.containsKey(v.getData())) {
                    docsWithErrors.add(doc.value);
                    //onError.accept("key must not exist " + Arrays.toString(v.getData()));
                }

//            if (it.hasNext()) {
//                Map.Entry<byte[], Long> e = it.next();
//                if (!Arrays.equals(e.getKey(), v.getData())) {
//                    onError.accept("wrong order of key " + Arrays.toString(v.getData()) + " vs " + Arrays.toString(e.getKey()));
//                }
//
//            } else {
//                onError.accept("key must not exist " + Arrays.toString(v.getData()));
//            }

                if (counter.incrementAndGet() % 10000000 == 0) {
                    System.out.println(counter.get());
                }

                return true;
            });
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.out.println(counter);
        onError.accept("Documents: " + docsTotal.size() + "; with errors: " + docsWithErrors.size());



//        while (it.hasNext()) {
//            onError.accept("index missing " + Arrays.toString(it.next().getKey()));
//        }

        return docsWithErrors;
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
