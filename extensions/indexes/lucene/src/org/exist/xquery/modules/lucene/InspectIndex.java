package org.exist.xquery.modules.lucene;

import java.io.IOException;
import java.net.URISyntaxException;

import org.exist.dom.DocumentImpl;
import org.exist.dom.QName;
import org.exist.indexing.lucene.LuceneIndex;
import org.exist.indexing.lucene.LuceneIndexWorker;
import org.exist.security.PermissionDeniedException;
import org.exist.storage.lock.Lock;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import static org.exist.xquery.Cardinality.*;
import static org.exist.xquery.modules.lucene.LuceneModule.PREFIX;
import static org.exist.xquery.modules.lucene.LuceneModule.NAMESPACE_URI;
import static org.exist.xquery.value.Type.*;

public class InspectIndex extends BasicFunction {

    private final static QName HAS_INDEX = new QName("has-index", NAMESPACE_URI, PREFIX);
    private final static QName IS_NODE_INDEXED = new QName("is-node-indexed", NAMESPACE_URI, PREFIX);

	public final static FunctionSignature[] signatures = {
        new FunctionSignature(
            HAS_INDEX,
            "Check if the given document has a lucene index defined on it. This method " +
            "will return true for both, indexes created via collection.xconf or manual index " +
            "fields added to the document with ft:index.",
            new SequenceType[] {
                new FunctionParameterSequenceType("path", STRING, EXACTLY_ONE,
                    "Full path to the resource to check")
            },
            new FunctionReturnSequenceType( BOOLEAN, ZERO_OR_MORE, "")
        ),
        new FunctionSignature(
            IS_NODE_INDEXED,
            "Check if the given node has a lucene index defined on it. This method " +
            "will return true there is indexes created via collection.xconf.",
            new SequenceType[] {
                new FunctionParameterSequenceType("node", NODE, EXACTLY_ONE,
                    "Node to check")
            },
            new FunctionReturnSequenceType( BOOLEAN, ZERO_OR_MORE, "")
        )
	};
	
	public InspectIndex(XQueryContext context, FunctionSignature signature) {
		super(context, signature);
	}

	@Override
	public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

        try {

            LuceneIndexWorker index = (LuceneIndexWorker)
                    context.getBroker().getIndexController().getWorkerByIndexId(LuceneIndex.ID);

            if (HAS_INDEX.equals(getName())) {

                String path = args[0].itemAt(0).getStringValue();

                // Retrieve document from database
                DocumentImpl doc = context.getBroker().getXMLResource(XmldbURI.xmldbUriFor(path), Lock.READ_LOCK);

                // Verify the document actually exists
                if (doc == null) {
                    throw new XPathException("Document " + path + " does not exist.");
                }

                return new BooleanValue(index.hasIndex(doc.getDocId()));

            } else {

                Item item = args[0].itemAt(0);

                if (!Type.subTypeOf(item.getType(), Type.NODE)) {
                    throw new XPathException(this, ErrorCodes.XPTY0004,
                            "Context item is not a node");
                }
                NodeValue node = (NodeValue) item;

                if (node.getImplementationType() != NodeValue.PERSISTENT_NODE) {
                    throw new XPathException(this, ErrorCodes.XPTY0004,
                            "Node must be persistent");
                }

                DocumentImpl doc = (DocumentImpl)node.getOwnerDocument();

                if (doc == null) {
                    throw new XPathException(this, "Node have no document.");
                }

                return new BooleanValue(index.hasIndex(doc.getDocId(), node.getNodeId()));
            }

		} catch (PermissionDeniedException e) {
			throw new XPathException(this, LuceneModule.EXXQDYFT0001, e.getMessage());
		} catch (URISyntaxException e) {
			throw new XPathException(this, LuceneModule.EXXQDYFT0003, e.getMessage());
		} catch (IOException e) {
			throw new XPathException(this, LuceneModule.EXXQDYFT0002, e.getMessage());
		}
	}

}
