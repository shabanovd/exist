/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2010 The eXist Project
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
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *  
 *  $Id: TryCatchExpression.java 13700 2011-01-30 13:34:44Z dizzzz $
 */
package org.exist.xquery;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
                
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.exist.Namespaces;
import org.exist.dom.QName;

import org.exist.security.xacml.XACMLSource;
import org.exist.xquery.ErrorCodes.ErrorCode;
import org.exist.xquery.ErrorCodes.JavaErrorCode;
import org.exist.xquery.util.ExpressionDumper;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.*;

/**
 * XQuery 3.0 try {...} catch{...} expression.
 * 
 * @author Adam Retter <adam@exist-db.org>
 * @author Leif-Jöran Olsson <ljo@exist-db.org>
 * @author Dannes Wessels <dannes@exist-db.org>
 */
public class TryCatchExpression extends AbstractExpression {

    private static final Logger LOG = LogManager.getLogger(TryCatchExpression.class);

    private static final QName QN_CODE = new QName("code", Namespaces.W3C_XQUERY_XPATH_ERROR_NS, Namespaces.W3C_XQUERY_XPATH_ERROR_PREFIX);
    private static final QName QN_DESCRIPTION = new QName("description", Namespaces.W3C_XQUERY_XPATH_ERROR_NS, Namespaces.W3C_XQUERY_XPATH_ERROR_PREFIX);
    private static final QName QN_VALUE = new QName("value", Namespaces.W3C_XQUERY_XPATH_ERROR_NS, Namespaces.W3C_XQUERY_XPATH_ERROR_PREFIX);
    private static final QName QN_MODULE = new QName("module", Namespaces.W3C_XQUERY_XPATH_ERROR_NS, Namespaces.W3C_XQUERY_XPATH_ERROR_PREFIX);
    private static final QName QN_LINE_NUM = new QName("line-number", Namespaces.W3C_XQUERY_XPATH_ERROR_NS, Namespaces.W3C_XQUERY_XPATH_ERROR_PREFIX);
    private static final QName QN_COLUMN_NUM = new QName("column-number", Namespaces.W3C_XQUERY_XPATH_ERROR_NS, Namespaces.W3C_XQUERY_XPATH_ERROR_PREFIX);
    private static final QName QN_ADDITIONAL = new QName("additional", Namespaces.W3C_XQUERY_XPATH_ERROR_NS, Namespaces.W3C_XQUERY_XPATH_ERROR_PREFIX);

    private static final QName QN_XQUERY_STACK_TRACE = new QName("xquery-stack-trace", Namespaces.EXIST_XQUERY_XPATH_ERROR_NS, Namespaces.EXIST_XQUERY_XPATH_ERROR_PREFIX);
    private static final QName QN_JAVA_STACK_TRACE = new QName("java-stack-trace", Namespaces.EXIST_XQUERY_XPATH_ERROR_NS, Namespaces.EXIST_XQUERY_XPATH_ERROR_PREFIX);

    private final Expression tryTargetExpr;
    private final List<CatchClause> catchClauses = new ArrayList<>();

    /**
     *  Constructor.
     * 
     * @param context   Xquery context
     * @param tryTargetExpr Expression to be evaluated
     */
    public TryCatchExpression(final XQueryContext context, final Expression tryTargetExpr) {
        super(context);
        this.tryTargetExpr = tryTargetExpr;
    }

    /**
     * Receive catch-clause data from parser.
     *
     * TODO: List<String> must be changed to List<QName>
     */
    public void addCatchClause(List<String> catchErrorList, List<QName> catchVars, Expression catchExpr) {
        catchClauses.add( new CatchClause(catchErrorList, catchVars, catchExpr) );
    }

    @Override
    public int getDependencies() {
        return Dependency.CONTEXT_SET | Dependency.CONTEXT_ITEM;
    }

    public Expression getTryTargetExpr() {
        return tryTargetExpr;
    }

    public List<CatchClause> getCatchClauses() {
        return catchClauses;
    }

    @Override
    public int getCardinality() {
        return Cardinality.ZERO_OR_MORE;
    }

    @Override
    public void analyze(final AnalyzeContextInfo contextInfo) throws XPathException {
        contextInfo.setFlags(contextInfo.getFlags() & (~IN_PREDICATE));
        contextInfo.setParent(this);
        tryTargetExpr.analyze(contextInfo);
        for (final CatchClause catchClause : catchClauses) {
            catchClause.getCatchExpr().analyze(contextInfo);
        }
    }

    @Override
    public Sequence eval(final Sequence contextSequence, final Item contextItem) throws XPathException {

        context.expressionStart(this);

        if(getContext().getXQueryVersion()<30){
            throw new XPathException(this, ErrorCodes.EXXQDY0003, "The try-catch expression is only available in xquery version \"3.0\" and later.");
        }

        try {
            // Evaluate 'try' expression
            final Sequence tryTargetSeq = tryTargetExpr.eval(contextSequence, contextItem);
            return tryTargetSeq;

        } catch (final Throwable throwable) { 

            final ErrorCode errorCode;

            // fn:error throws an XPathException
            if(throwable instanceof XPathException){
                // Get errorcode from nicely thrown xpathexception
                final XPathException xpe = (XPathException)throwable;

                if(xpe.getErrorCode() != null) {
                    if(xpe.getErrorCode() == ErrorCodes.ERROR) {
                        errorCode = extractErrorCode(xpe);
                    } else {
                        errorCode = xpe.getErrorCode();
                    }
                } else {
                    // if no errorcode is found, reconstruct by parsing the error text.
                    errorCode = extractErrorCode(xpe);
                }
            } else {
                // Get errorcode from all other errors and exceptions
                errorCode = new JavaErrorCode(throwable);
            }

            // We need the qname in the end
            final QName errorCodeQname = errorCode.getErrorQName();

            // Exception in thrown, catch expression will be evaluated.
            // catchvars (CatchErrorCode (, CatchErrorDesc (, CatchErrorVal)?)? )
            // need to be retrieved as variables
            Sequence catchResultSeq = null;
            final LocalVariable mark0 = context.markLocalVariables(false); // DWES: what does this do?
            
            // Register new namespace
            // DWES: 
            // when declaring "fn:error( fn:QName('http://www.w3.org/2005/xqt-errors', 'err:FOER0000') )"
            // An Exception is thrown: err:XQST0033 It is a static error if a module contains multiple bindings for the same namespace prefix.
            // DWES: should I use popLocalVariables
            context.declareInScopeNamespace(Namespaces.W3C_XQUERY_XPATH_ERROR_PREFIX, Namespaces.W3C_XQUERY_XPATH_ERROR_NS);
            context.declareInScopeNamespace(Namespaces.EXIST_XQUERY_XPATH_ERROR_PREFIX, Namespaces.EXIST_XQUERY_XPATH_ERROR_NS);
            
            //context.declareInScopeNamespace(null, null);

            try {
                // flag used to escape loop when errorcode has matched
                boolean errorMatched = false;

                // Iterate on all catch clauses
                for (final CatchClause catchClause : catchClauses) {
                    
                    if (isErrorInList(errorCodeQname, catchClause.getCatchErrorList()) && !errorMatched) {

                        errorMatched = true;

                        // Get catch variables
                        final LocalVariable mark1 = context.markLocalVariables(false); // DWES: what does this do?
                        
                        try {  
                            // Add std errors
                            addErrCode(errorCodeQname);                          
                            addErrDescription(throwable, errorCode);
                            addErrValue(throwable);
                            addErrModule(throwable);
                            addErrLineNumber(throwable);
                            addErrColumnNumber(throwable);
                            addErrAdditional(throwable);
                            addFunctionTrace(throwable);
                            addJavaTrace(throwable);

                            // Evaluate catch expression
                            catchResultSeq = ((Expression) catchClause.getCatchExpr()).eval(contextSequence, contextItem);
                            
                            
                        } finally {
                            context.popLocalVariables(mark1, catchResultSeq);
                        }

                    } else {
                        // if in the end nothing is set, rethrow after loop
                    }
                } // for catch clauses

                // If an error hasn't been caught, throw new one
                if (!errorMatched) {
                    LOG.error(throwable.getMessage(), throwable);
                    throw new XPathException(throwable);
                }


            } finally {
                context.popLocalVariables(mark0, catchResultSeq);
            }

            return catchResultSeq;

        } finally {
            context.expressionEnd(this);
        }
    }


    // err:additional	item()*	
    // Implementation-defined. This variable must be bound so that a query 
    // can reference it without raising an error. The purpose of this 
    // variable is to allow implementations to provide any additional 
    // information that might be useful.
    private void addErrAdditional(final Throwable t) throws XPathException {
        final LocalVariable err_additional = new LocalVariable(QN_ADDITIONAL);
        err_additional.setSequenceType(new SequenceType(Type.ITEM, Cardinality.ZERO_OR_ONE));
        err_additional.setValue(Sequence.EMPTY_SEQUENCE);

        context.declareVariableBinding(err_additional);
    }

    // err:column-number	xs:integer?	
    // The column number within the stylesheet module of the instruction 
    // where the error occurred, or an empty sequence if the information 
    // is not available. The value may be approximate.
    private void addErrColumnNumber(final Throwable t) throws XPathException {
        final LocalVariable err_column_nr = new LocalVariable(QN_COLUMN_NUM);
        err_column_nr.setSequenceType(new SequenceType(Type.INTEGER, Cardinality.ZERO_OR_ONE));

        final Sequence colNum;
        if (t != null && t instanceof XPathException) {
            colNum = new IntegerValue(((XPathException)t).getColumn());
        } else {
            colNum = Sequence.EMPTY_SEQUENCE;
        }
        err_column_nr.setValue(colNum);

        context.declareVariableBinding(err_column_nr);
    }

    // err:line-number	xs:integer?	
    // The line number within the stylesheet module of the instruction 
    // where the error occurred, or an empty sequence if the information 
    // is not available. The value may be approximate.
    private void addErrLineNumber(final Throwable t) throws XPathException {
        final LocalVariable err_line_nr = new LocalVariable(QN_LINE_NUM);
        err_line_nr.setSequenceType(new SequenceType(Type.INTEGER, Cardinality.ZERO_OR_ONE));

        final Sequence lineNum;
        if (t != null && t instanceof XPathException) {
            lineNum = new IntegerValue(((XPathException)t).getLine());
        } else {
            lineNum = Sequence.EMPTY_SEQUENCE;
        }
        err_line_nr.setValue(lineNum);

        context.declareVariableBinding(err_line_nr);
    }

    // err:module	xs:string?	
    // The URI (or system ID) of the module containing the expression 
    // where the error occurred, or an empty sequence if the information 
    // is not available.
    private void addErrModule(final Throwable t) throws XPathException {
        final LocalVariable err_module = new LocalVariable(QN_MODULE);
        err_module.setSequenceType(new SequenceType(Type.STRING, Cardinality.ZERO_OR_ONE));

        final Sequence module;
        if (t != null && t instanceof XPathException && ((XPathException)t).getXACMLSource() != null) {
            module = new StringValue(((XPathException)t).getXACMLSource().getKey());
        } else {
            module = Sequence.EMPTY_SEQUENCE;
        }
        err_module.setValue(module);

        context.declareVariableBinding(err_module);
    }

    // err:value	item()*	
    // Value associated with the error. For an error raised by calling 
    // the error function, this is the value of the third  argument 
    // (if supplied).
    private void addErrValue(final Throwable t) throws XPathException {
        final LocalVariable err_value = new LocalVariable(QN_VALUE);
        err_value.setSequenceType(new SequenceType(Type.ITEM, Cardinality.ZERO_OR_MORE));

        final Sequence errorValue;
        if (t != null) {
            // Get error value from exception
            if(t instanceof XPathException && ((XPathException)t).getErrorVal() != null) {
                errorValue = ((XPathException)t).getErrorVal();
            } else {
                errorValue = Sequence.EMPTY_SEQUENCE;
            }
        } else {
            // fill data from throwable object
            try {
                errorValue = new StringValue(getStackTrace(t));
            } catch(final IOException ioe) {
                throw new XPathException(ioe);
            }
        }
        err_value.setValue(errorValue);

        context.declareVariableBinding(err_value);
    }

    // err:description	xs:string?	
    // A description of the error condition; an empty sequence if no 
    // description is available (for example, if the error function 
    // was called with one argument).
    private void addErrDescription(final Throwable t, final ErrorCode errorCode) throws XPathException {
        String description = errorCode.getDescription();
        if (description == null && t != null) {
            if(t instanceof XPathException) {
                description = ((XPathException) t).getDetailMessage();
            } else {
                description = t.getMessage();
            }
        }

        final LocalVariable err_description = new LocalVariable(QN_DESCRIPTION);
        err_description.setSequenceType(new SequenceType(Type.QNAME, Cardinality.ZERO_OR_ONE));
        if(description == null){
            err_description.setValue(Sequence.EMPTY_SEQUENCE);
        } else {
            err_description.setValue(new StringValue(description));
        } 
        context.declareVariableBinding(err_description);
    }

    // err:code	xs:QName	
    // The error code
    private void addErrCode(final QName errorCodeQname) throws XPathException {
        final LocalVariable err_code = new LocalVariable(QN_CODE);
        err_code.setSequenceType(new SequenceType(Type.QNAME, Cardinality.EXACTLY_ONE));
        err_code.setValue(new QNameValue(context, errorCodeQname));
        context.declareVariableBinding(err_code);
    }

    @Override
    public void dump(final ExpressionDumper dumper) {
        dumper.display("try {");
        dumper.startIndent();
        tryTargetExpr.dump(dumper);
        dumper.endIndent();
        for (final CatchClause catchClause : catchClauses) {
            final Expression catchExpr = (Expression) catchClause.getCatchExpr();
            dumper.nl().display("} catch (expr) {");
            dumper.startIndent();
            catchExpr.dump(dumper);
            dumper.nl().display("}");
            dumper.endIndent();
        }
    }

    /**
     *  Extract and construct errorcode from error text.
     */
    private ErrorCode extractErrorCode(final XPathException xpe)  {

        // Get message from string
        final String message = xpe.getMessage();

        // if the 9th position has a ":" it is probably a custom error text
        if (':' == message.charAt(8)) {

            final String[] data = extractLocalName(xpe.getMessage());
            final ErrorCode errorCode = new ErrorCode(data[0], data[1]);
            LOG.debug("Parsed string '" + xpe.getMessage() + "' for Errorcode. "
                    + "Qname='" + data[0] + "' message='" + data[1] + "'");
            return errorCode;

        }

        // Convert xpe to Throwable
        Throwable retVal = xpe;

        // Swap with cause if present
        Throwable cause = xpe.getCause();
        if(cause != null && !(cause instanceof XPathException) ){
            retVal = cause;
        }

        // Fallback, create java error
        return new ErrorCodes.JavaErrorCode(retVal);
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder();
        result.append("try { ");
        result.append(tryTargetExpr.toString());
        for (final CatchClause catchClause : catchClauses) {
            final Expression catchExpr = (Expression) catchClause.getCatchExpr();
            result.append(" } catch (expr) { ");
            result.append(catchExpr.toString());
            result.append("}");
        }
        return result.toString();
    }

    /* (non-Javadoc)
     * @see org.exist.xquery.Expression#returnsType()
     */
    @Override
    public int returnsType() {
        // fixme! /ljo
        return ((Expression) catchClauses.get(0).getCatchExpr()).returnsType();
    }

    /* (non-Javadoc)
     * @see org.exist.xquery.AbstractExpression#resetState()
     */
    @Override
    public void resetState(final boolean postOptimization) {
        super.resetState(postOptimization);
        tryTargetExpr.resetState(postOptimization);
        for (final CatchClause catchClause : catchClauses) {
            final Expression catchExpr = (Expression) catchClause.getCatchExpr();
            catchExpr.resetState(postOptimization);
        }
    }

    @Override
    public void accept(final ExpressionVisitor visitor) {
        visitor.visitTryCatch(this);
    }

    /**
     *  Check if error parameter matches list of error names.
     * An '*' matches immediately.
     * 
     * @return TRUE is qname is in list, or list contains '*', else FALSE,
     */
    private boolean isErrorInList(final QName error, final List<String> errors) {

        final String qError = error.getStringValue();
        for (final String lError : errors) {
            if ("*".equals(lError)) {
                return true;
            }

            if (qError.equals(lError)) {
                return true;
            }
        }
        return false;
    }

    private String[] extractLocalName(final String errorText)
            throws IllegalArgumentException {
        final int p = errorText.indexOf(':');
        if (p == Constants.STRING_NOT_FOUND) {
            return new String[]{null, errorText};
        }

        return new String[]{errorText.substring(0, p).trim(), errorText.substring(p + 1).trim()};
    }

    /**
     * Write stacktrace to String. 
     */
    private String getStackTrace(final Throwable t ) throws IOException {
		if (t == null) {
            return null;
        }

        try(final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw)) {

            t.printStackTrace(pw);
            pw.flush();
            return sw.toString();
        }
    }

    private void addFunctionTrace(final Throwable t) throws XPathException {
        final LocalVariable localVar = new LocalVariable(QN_XQUERY_STACK_TRACE);
        localVar.setSequenceType(new SequenceType(Type.STRING, Cardinality.ZERO_OR_MORE));

        final Sequence trace;
		if(t != null && t instanceof XPathException) {
			final List<XPathException.FunctionStackElement> callStack = ((XPathException)t).getCallStack();
			if(callStack == null){
				trace = Sequence.EMPTY_SEQUENCE;
			} else {
				final Sequence result = new ValueSequence();
				for(final XPathException.FunctionStackElement elt : callStack){
					result.add(new StringValue("at " + elt.toString()) );
				}
				trace = result;
			}
        } else {
            trace = Sequence.EMPTY_SEQUENCE;
        }
        localVar.setValue(trace);

        context.declareVariableBinding(localVar);
    }
    
    
    private void addJavaTrace(final Throwable t) throws XPathException  {
        final LocalVariable localVar = new LocalVariable(QN_JAVA_STACK_TRACE);
        localVar.setSequenceType(new SequenceType(Type.QNAME, Cardinality.ZERO_OR_MORE));

        final Sequence trace;
		if (t != null && t.getStackTrace() != null) {
            final Sequence result = new ValueSequence();
            addJavaTrace(t, result);
            trace = result;
		} else {
            trace = Sequence.EMPTY_SEQUENCE;
        }
        localVar.setValue(trace);

        context.declareVariableBinding(localVar);
    }
    
    // Local recursive function
    private void addJavaTrace(final Throwable t, final Sequence result) throws XPathException {
        final StackTraceElement[] elements = t.getStackTrace();
        result.add(new StringValue("Caused by: " + t.toString()));
        for (final StackTraceElement elt : elements) {
            result.add(new StringValue("at " + elt.toString()));
        }

        final Throwable cause = t.getCause();
        if (cause != null) {
            addJavaTrace(cause, result);
        }
    }


    /**
     * Data container
     */
    public class CatchClause {

        private List<String> catchErrorList = null;
        private List<QName> catchVars = null;
        private Expression catchExpr = null;

        public CatchClause(List<String> catchErrorList, List<QName> catchVars, Expression catchExpr) {
            this.catchErrorList = catchErrorList;
            this.catchVars = catchVars;
            this.catchExpr = catchExpr;
        }

        public List<String> getCatchErrorList() {
            return catchErrorList;
        }

        public void setCatchErrorList(List<String> catchErrorList) {
            this.catchErrorList = catchErrorList;
        }

        public Expression getCatchExpr() {
            return catchExpr;
        }

        public void setCatchExpr(Expression catchExpr) {
            this.catchExpr = catchExpr;
        }

        public List<QName> getCatchVars() {
            return catchVars;
        }

        public void setCatchVars(List<QName> catchVars) {
            this.catchVars = catchVars;
        }
    }
}
