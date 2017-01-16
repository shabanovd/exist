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
package org.exist.xquery.functions.system;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.dom.DefaultDocumentSet;
import org.exist.dom.DocumentImpl;
import org.exist.dom.MutableDocumentSet;
import org.exist.dom.QName;
import org.exist.storage.NativeBroker;
import org.exist.storage.structural.CheckerStructuralIndex;
import org.exist.storage.structural.NativeStructuralIndexWorker;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class CheckStructuralIndex extends BasicFunction {
    protected static final Logger logger = LogManager.getLogger(CheckStructuralIndex.class);
    public final static FunctionSignature signature =
        new FunctionSignature(
            new QName("check-structural-index", SystemModule.NAMESPACE_URI, SystemModule.PREFIX),
            "Check structural index. " ,
            FunctionSignature.NO_ARGS,
            new SequenceType(Type.STRING, Cardinality.ZERO_OR_MORE)
        );

    public CheckStructuralIndex(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        if( !context.getSubject().hasDbaRole() )
            throw new XPathException( this,
                "Permission denied, calling user '" + context.getSubject().getName() + "' must be a DBA");

        NativeBroker broker = (NativeBroker)context.getBroker();
        NativeStructuralIndexWorker worker = (NativeStructuralIndexWorker)broker.getStructuralIndex();

//        ValueSequence res = new ValueSequence();

        AtomicLong count = new AtomicLong();

        Consumer<String> errors = err -> {
            System.out.println(err);
            count.incrementAndGet();
            //res.add(new StringValue(err));

            if (count.get() > 10) throw new RuntimeException();
        };

        Path data = Paths.get("check_structure_index");

        if (Files.exists(data)) {
            try (CheckerStructuralIndex checker = new CheckerStructuralIndex(worker, data)) {

                Set<Integer> docIds = checker.runChecker(errors);

                MutableDocumentSet docs = new DefaultDocumentSet();
                broker.getAllXMLResources(docs);

                System.out.println(docs.getDocumentCount());

                int notFount = 0;
                for (Integer id : docIds) {
                    DocumentImpl doc = docs.getDoc(id);
                    if (doc == null) {
//                        System.out.println(id);
                        notFount++;
                    } else {
                        System.out.println(docs.getDoc(id).getDocumentURI());
                    }
                }
                System.out.println(docs.getDocumentCount() + ", not found "+notFount);

            } catch (Exception e) {
                throw new XPathException(this, e);
            }

        } else {
            try (CheckerStructuralIndex checker = new CheckerStructuralIndex(worker, data)) {
                broker.index(
                    broker.getCollection(XmldbURI.ROOT_COLLECTION_URI),
                    checker.stream(),
                    errors
                );

                checker.runChecker(errors);

            } catch (Exception e) {
                throw new XPathException(this, e);
            }
        }

        return new DecimalValue(String.valueOf(count.get()));
    }
}
