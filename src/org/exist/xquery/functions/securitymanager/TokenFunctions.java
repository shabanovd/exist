/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2015 The eXist Project
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
package org.exist.xquery.functions.securitymanager;

import org.exist.dom.QName;
import org.exist.memtree.MemTreeBuilder;
import org.exist.security.Subject;
import org.exist.security.internal.TokenRecord;
import org.exist.security.internal.TokensManager;
import org.exist.xquery.*;
import org.exist.xquery.value.*;
import org.xml.sax.helpers.AttributesImpl;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class TokenFunctions extends BasicFunction {

    private final static QName qnCreate = new QName("create-token", SecurityManagerModule.NAMESPACE_URI, SecurityManagerModule.PREFIX);
    private final static QName qnList = new QName("list-tokens", SecurityManagerModule.NAMESPACE_URI, SecurityManagerModule.PREFIX);
    private final static QName qnInvalidate = new QName("invalidate-token", SecurityManagerModule.NAMESPACE_URI, SecurityManagerModule.PREFIX);

    public final static FunctionSignature FN_CREATE = new FunctionSignature(
        qnCreate,
        "Create token for current account",
        null,
        new SequenceType(Type.STRING, Cardinality.ONE)
    );

    public final static FunctionSignature FN_LIST = new FunctionSignature(
        qnList,
        "List tokens for current account",
        null,
        new SequenceType(Type.NODE, Cardinality.ZERO_OR_MORE)
    );

    public final static FunctionSignature FN_INVALIDATE = new FunctionSignature(
        qnInvalidate,
        "Invalidate token",
        new SequenceType[] {
            new FunctionParameterSequenceType("token", Type.STRING, Cardinality.EXACTLY_ONE, "token")
        },
        new SequenceType(Type.BOOLEAN, Cardinality.ONE)
    );

    public TokenFunctions(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

//        if (context.getSubject().hasDbaRole() )
//            throw new XPathException(this, "Account with DBA access can't have token");

        TokensManager tm = context.getDatabase().getSecurityManager().tokenManager();
        if (tm == null)
            throw new XPathException(this, "no tokens manager");

        Subject subject = context.getSubject();
        if (!subject.isAuthenticated())
            throw new XPathException(this, "Login first");

        if (mySignature == FN_CREATE) {
            return new StringValue(tm.createToken(subject));

        } else if (mySignature == FN_LIST) {

            Sequence result = new ValueSequence();

            for (TokenRecord record : tm.list(subject))
                result.add(record.toXml());

            return result;

        } else if (mySignature == FN_INVALIDATE) {
            return new BooleanValue(tm.invalidateToken(args[0].getStringValue()));
        }

        throw new XPathException(this, "unknown function "+mySignature);
    }
}
