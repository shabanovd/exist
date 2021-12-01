package org.exist.xquery.fixes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.collections.Collection;
import org.exist.dom.DocumentImpl;
import org.exist.dom.QName;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.Txn;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.*;
import org.exist.xquery.functions.xmldb.XMLDBModule;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

public class CleanupRenameDocument extends BasicFunction {
    protected static final Logger logger = LogManager.getLogger(CleanupRenameDocument.class);
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("cleanup-rename", Module.NAMESPACE_URI, Module.PREFIX),
            "Rename the resource $resource from the collection $collection-uri. " +
                XMLDBModule.COLLECTION_URI,
            new SequenceType[]{
                new FunctionParameterSequenceType("collection-uri", Type.STRING, Cardinality.EXACTLY_ONE, "The collection URI"),
                new FunctionParameterSequenceType("resource", Type.STRING, Cardinality.EXACTLY_ONE, "The resource"),
                new FunctionParameterSequenceType("new-name", Type.STRING, Cardinality.EXACTLY_ONE, "The new name")},
            new SequenceType(Type.ITEM, Cardinality.EMPTY)
        )
    };

    public CleanupRenameDocument(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        if( !context.getSubject().hasDbaRole() )
            throw new XPathException( this,
                "Permission denied, calling user '" + context.getSubject().getName() + "' must be a DBA");

        final XmldbURI colURL = XmldbURI.create(args[0].itemAt(0).getStringValue());
        final XmldbURI docURL = XmldbURI.createInternal(args[1].itemAt(0).getStringValue());
        final XmldbURI newURL = XmldbURI.createInternal(args[2].itemAt(0).getStringValue());

        DBBroker broker = context.getBroker();

        try (Txn tx = broker.beginTx()) {

            Collection col = broker.getCollection(colURL);

            if (col == null) throw new XPathException(this, "collection not found");

            DocumentImpl doc = col.getDocument(broker, docURL);

            if (doc == null) throw new XPathException(this, "document not found");

            broker.moveResource(tx, doc, col, newURL);
            broker.saveCollection(tx, col);

            tx.success();

        } catch (Exception e) {
            throw new XPathException(this, e);
        }

        return Sequence.EMPTY_SEQUENCE;
    }
}
