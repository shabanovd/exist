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

import org.apache.log4j.Logger;
import org.exist.dom.QName;
import org.exist.storage.NativeBroker;
import org.exist.storage.structural.CheckerStructuralIndex;
import org.exist.storage.structural.NativeStructuralIndexWorker;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import java.util.function.Consumer;

public class CheckStructuralIndex extends BasicFunction {
    protected static final Logger logger = Logger.getLogger(CheckStructuralIndex.class);
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

        ValueSequence res = new ValueSequence();

        Consumer<String> errors = err -> res.add(new StringValue(err));

        try (CheckerStructuralIndex checker = new CheckerStructuralIndex(worker)) {
            broker.index(
                broker.getCollection(XmldbURI.ROOT_COLLECTION_URI),
                checker.stream(),
                errors
            );

            checker.runChecker(errors);

        } catch (Exception e) {
            throw new XPathException(this, e);
        }

        return res;
    }
}
