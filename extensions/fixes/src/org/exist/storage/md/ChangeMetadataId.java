/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2018 The eXist Project
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
package org.exist.storage.md;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.dom.QName;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.BasicFunction;
import org.exist.xquery.Cardinality;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.fixes.Module;
import org.exist.xquery.functions.xmldb.XMLDBModule;
import org.exist.xquery.value.BooleanValue;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.Type;

public class ChangeMetadataId extends BasicFunction {
    protected static final Logger logger = LogManager.getLogger(ChangeMetadataId.class);
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            new QName("change-metadata-id", Module.NAMESPACE_URI, Module.PREFIX),
            "Force set metadata the resource $resource from the collection $collection-uri. " +
                XMLDBModule.COLLECTION_URI,
            new SequenceType[]{
                new FunctionParameterSequenceType("uri", Type.STRING, Cardinality.EXACTLY_ONE, "The resource URI"),
                new FunctionParameterSequenceType("uuid", Type.STRING, Cardinality.EXACTLY_ONE, "UUID")},
            new SequenceType(Type.ITEM, Cardinality.EMPTY)
        )
    };

    public ChangeMetadataId(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        if( !context.getSubject().hasDbaRole() )
            throw new XPathException( this,
                "Permission denied, calling user '" + context.getSubject().getName() + "' must be a DBA");

        final XmldbURI url = XmldbURI.create(args[0].itemAt(0).getStringValue());
        final String uuid = args[1].itemAt(0).getStringValue();

        MetaData md = MetaData.get();

        Metas cc = md.getMetas(uuid);
        if (cc != null) {
            return BooleanValue.valueOf(cc.getURI().equals(url.toString()));
        }

        md.delMetas(url);

        Metas cre = md._addMetas(url.toString(), uuid);

        return BooleanValue.valueOf(cre != null);
    }
}
