///*
// * eXist Open Source Native XML Database
// * Copyright (C) 2001-2017 The eXist Project
// * http://exist-db.org
// *
// * This program is free software; you can redistribute it and/or
// * modify it under the terms of the GNU Lesser General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Lesser General Public License for more details.
// *
// * You should have received a copy of the GNU Lesser General Public
// * License along with this library; if not, write to the Free Software
// * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
// */
//package org.exist.xquery.fixes;
//
//import java.io.File;
//import java.io.InputStream;
//import java.nio.file.Files;
//import java.nio.file.StandardCopyOption;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.exist.collections.Collection;
//import org.exist.dom.BinaryDocument;
//import org.exist.dom.DocumentImpl;
//import org.exist.dom.QName;
//import org.exist.revisions.RCSHolder;
//import org.exist.revisions.RCSManager;
//import org.exist.revisions.RCSResource;
//import org.exist.revisions.Revision;
//import org.exist.storage.DBBroker;
//import org.exist.storage.txn.Txn;
//import org.exist.xmldb.XmldbURI;
//import org.exist.xquery.BasicFunction;
//import org.exist.xquery.Cardinality;
//import org.exist.xquery.FunctionSignature;
//import org.exist.xquery.XPathException;
//import org.exist.xquery.XQueryContext;
//import org.exist.xquery.functions.xmldb.XMLDBModule;
//import org.exist.xquery.value.FunctionParameterSequenceType;
//import org.exist.xquery.value.Sequence;
//import org.exist.xquery.value.SequenceType;
//import org.exist.xquery.value.Type;
//
//public class CleanupRestoreBinaryFromRCS extends BasicFunction {
//    protected static final Logger logger = LogManager.getLogger(CleanupRestoreBinaryFromRCS.class);
//    public final static FunctionSignature signatures[] = {
//        new FunctionSignature(
//            new QName("cleanup-restore-binary-from-RCS", Module.NAMESPACE_URI, Module.PREFIX),
//            "Restore binary resource fs state from RCS commit. " +
//                XMLDBModule.COLLECTION_URI,
//            new SequenceType[]{
//                new FunctionParameterSequenceType("organization-id", Type.STRING, Cardinality.EXACTLY_ONE, "The organization id"),
//
//                new FunctionParameterSequenceType("collection-uri", Type.STRING, Cardinality.EXACTLY_ONE, "The collection URI"),
//                new FunctionParameterSequenceType("resource", Type.STRING, Cardinality.EXACTLY_ONE, "The resource"),
//
//                new FunctionParameterSequenceType("resource-uuid", Type.STRING, Cardinality.EXACTLY_ONE, "The resource UUID"),
//                new FunctionParameterSequenceType("revision-id", Type.STRING, Cardinality.EXACTLY_ONE, "The resource revision id")
//
//            },
//            new SequenceType(Type.ITEM, Cardinality.EMPTY)
//        ),
//        new FunctionSignature(
//            new QName("cleanup-restore-binary-from-last-RCS-revision", Module.NAMESPACE_URI, Module.PREFIX),
//            "Restore binary resource fs state from RCS commit. " +
//                XMLDBModule.COLLECTION_URI,
//            new SequenceType[]{
//                new FunctionParameterSequenceType("organization-id", Type.STRING, Cardinality.EXACTLY_ONE, "The organization id"),
//
//                new FunctionParameterSequenceType("collection-uri", Type.STRING, Cardinality.EXACTLY_ONE, "The collection URI"),
//                new FunctionParameterSequenceType("resource", Type.STRING, Cardinality.EXACTLY_ONE, "The resource"),
//
//                new FunctionParameterSequenceType("resource-uuid", Type.STRING, Cardinality.EXACTLY_ONE, "The resource UUID"),
//
//            },
//            new SequenceType(Type.ITEM, Cardinality.EMPTY)
//        )
//    };
//
//    public CleanupRestoreBinaryFromRCS(XQueryContext context, FunctionSignature signature) {
//        super(context, signature);
//    }
//
//    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
//        if( !context.getSubject().hasDbaRole() )
//            throw new XPathException( this,
//                "Permission denied, calling user '" + context.getSubject().getName() + "' must be a DBA");
//
//        final String oid = args[0].getStringValue();
//        final RCSHolder holder = RCSManager.get().getHolder(oid);
//
//        if (holder == null) throw new XPathException(this, "No organisation  '"+oid+"'.");
//
//        final XmldbURI colURL = XmldbURI.create(args[1].itemAt(0).getStringValue());
//        final XmldbURI docURL = XmldbURI.createInternal(args[2].itemAt(0).getStringValue());
//
//        final String uuid = args[3].getStringValue();
//
//        DBBroker broker = context.getBroker();
//
//        try {
//            Collection col = broker.getCollection(colURL);
//
//            if (col == null) throw new XPathException(this, "collection not found");
//
//            DocumentImpl doc = col.getDocument(broker, docURL);
//
//            if (doc == null) throw new XPathException(this, "document not found");
//
//            if (doc instanceof BinaryDocument) {
//
//                File file = broker.getBinaryFile((BinaryDocument) doc);
//
//                RCSResource resource = holder.resource(uuid);
//
//                Revision rev;
//
//                if (args.length >= 5) {
//                    final long revId = Long.valueOf(args[4].getStringValue());
//                    rev = resource.revision(revId);
//
//                } else {
//                    rev = resource.lastRevision();
//                }
//
//                if (rev != null) {
//                    try (InputStream stream = rev.getData()) {
//                        Files.copy(stream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
//                    }
//                }
//            }
//
//        } catch (Exception e) {
//            throw new XPathException(this, e);
//        }
//
//        return Sequence.EMPTY_SEQUENCE;
//    }
//}
