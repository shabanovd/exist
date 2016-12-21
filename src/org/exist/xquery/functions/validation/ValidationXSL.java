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
package org.exist.xquery.functions.validation;

import java.io.*;

import net.sf.saxon.s9api.*;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.IOUtils;
import org.apache.xml.resolver.CatalogManager;
import org.apache.xml.resolver.tools.CatalogResolver;
import org.exist.memtree.MemTreeBuilder;
import org.exist.validation.ValidationReport;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

import javax.xml.parsers.*;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import javax.xml.transform.Result;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;

import java.net.URL;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.saxon.dom.NodeOverNodeInfo;
import net.sf.saxon.om.NodeInfo;
import org.exist.xquery.value.SequenceType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class ValidationXSL extends BasicFunction  {

    private static final String EXTENDED_FUNCTION_TEXT = "Validate XML by using a specific grammar.";

    private static final String GRAMMAR_DESCRIPTION = "The reference to an OASIS catalog file (.xml), "
            + "a collection (path ends with '/') or a grammar document. "
            + "Supported grammar document extension is \".sch\". "
            + "The parameter can be passed as an xs:anyURI or a document node.";

    private static final String INSTANCE_DESCRIPTION = "The document referenced as xs:anyURI or a node (element or returned by fn:doc())";

    private static final String XML_REPORT_RETURN = " An XML report is returned.";

    // Setup function signature
    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
                new org.exist.dom.QName("validate-by-xsl-report", ValidationModule.NAMESPACE_URI,
                        ValidationModule.PREFIX),
                EXTENDED_FUNCTION_TEXT+XML_REPORT_RETURN,
                new SequenceType[]{
                        new FunctionParameterSequenceType("instance", Type.ITEM, Cardinality.EXACTLY_ONE, INSTANCE_DESCRIPTION),
                        new FunctionParameterSequenceType("grammar", Type.ITEM, Cardinality.EXACTLY_ONE, GRAMMAR_DESCRIPTION)
                },
                new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, Shared.xmlreportText)
        ),
        new FunctionSignature(
            new org.exist.dom.QName("validate-by-xsl-report", ValidationModule.NAMESPACE_URI,
            ValidationModule.PREFIX),
            EXTENDED_FUNCTION_TEXT+XML_REPORT_RETURN,
            new SequenceType[]{
                new FunctionParameterSequenceType("instance", Type.ITEM, Cardinality.EXACTLY_ONE, INSTANCE_DESCRIPTION),
                new FunctionParameterSequenceType("grammar", Type.ITEM, Cardinality.EXACTLY_ONE, GRAMMAR_DESCRIPTION),
                new FunctionParameterSequenceType("phase", Type.STRING, Cardinality.ONE, "The phase")
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, Shared.xmlreportText)
        )
    };

    public static final String ISO_SCHEMATRON_SVRL_NS = "http://purl.oclc.org/dsdl/svrl";
    private static final String INCLUDE_XSLT = "iso_dsdl_include.xsl";
    private static final String ABSTRACT_EXPAND_XSLT = "iso_abstract_expand.xsl";
    private static final String SVRL_REPORT_XSLT = "iso_svrl_for_xslt2.xsl";

    public ValidationXSL(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    /**
     * @throws XPathException
     * @see BasicFunction#eval(Sequence[], Sequence)
     */
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {

//        // Check input parameters
//        if (args.length != 1 && args.length != 2) {
//            return Sequence.EMPTY_SEQUENCE;
//        }

        ValidationReport report = new ValidationReport();

        try {
            Item schemaLocation = args[1].itemAt(0);

            String schemaHash = calcHash(Shared.getStreamSource(schemaLocation, context));
            String phase = args.length >= 3 ? args[2].getStringValue() : null;

            String key = schemaHash; //TODO: +"."+phase;

            CachedItem pair = checkCache(key);
            if (pair == null) {
                Processor processor = new Processor(false);
                //processor.setConfigurationProperty(FeatureKeys.RECOVERY_POLICY, Configuration.RECOVER_SILENTLY);

                StreamSource schema = Shared.getStreamSource(schemaLocation, context);
                XsltExecutable xslt = compileSchema(processor, schema, phase);

                pair = new CachedItem(processor, xslt);
            }

            Processor processor = pair.processor;
            XsltExecutable xslt = pair.xslt;

            XsltTransformer transformer = xslt.load();

            report.start();

            StreamSource data = Shared.getStreamSource(args[0].itemAt(0), context);
            validate(processor, transformer, data, true);

            report.stop();

            putToCache(key, pair);

        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            report.setException(ex);
        }


        final MemTreeBuilder builder = context.getDocumentBuilder();
        return Shared.writeReport(report, builder);
    }

    private static Map<String, Stack<CachedItem>> cache = new ConcurrentHashMap<>();
    private static long nextCheck = System.currentTimeMillis();

    private CachedItem checkCache(String key) {
        cleanupCheck();

        Stack<CachedItem> stack = cache.get(key);
        if (stack != null) {
            synchronized (stack) {
                if (!stack.isEmpty()) {
                    CachedItem item = stack.pop();
                    if (item != null) {
                        item.touch();
                    }
                    return item;
                }
            }
        }

        return null;
    }

    private void putToCache(String key, CachedItem pair) {
        cache.computeIfAbsent(key, k -> new Stack<>());

        Stack<CachedItem> stack = cache.get(key);
        if (stack != null) {
            synchronized (stack) {
                stack.push(pair);
            }
        }
    }

    private static long oneDay = 1000 * 60 * 60 * 24;

    private void cleanupCheck() {
        if (System.currentTimeMillis() > nextCheck) {
            nextCheck = System.currentTimeMillis() + oneDay;

            long ts = System.currentTimeMillis() - oneDay;

            cache.entrySet().removeIf(e -> {
                Stack<CachedItem> stack = e.getValue();
                if (stack == null) {
                    return true;
                }

                synchronized (stack) {
                    stack.removeIf(item -> item.lastAccessTime < ts);

                    return stack.isEmpty();
                }
            });
        }
    }

    final XsltExecutable compileStream(XsltCompiler compiler, String name) throws SaxonApiException, IOException {
        try (InputStream stream = ValidationXSL.class.getResourceAsStream(
                "iso-schematron-xslt2/"+name)) {

            System.out.println("stream: "+stream);
            return compiler.compile(new StreamSource(stream));
        }
    }

    final XsltExecutable compileSchema(Processor processor, Source schema, String phase) throws Exception {
        XsltCompiler compiler = processor.newXsltCompiler();
        //CatalogManager manager = new CatalogManager();
        CatalogManager manager = new CatalogManager("org/exist/xquery/functions/validation/CatalogManager.properties");
        compiler.setURIResolver(new CatalogResolver(manager));

        XsltExecutable includeXslt = compileStream(compiler, INCLUDE_XSLT);
        XsltExecutable abstractXslt = compileStream(compiler, ABSTRACT_EXPAND_XSLT);
        XsltExecutable svrlXslt = compileStream(compiler, SVRL_REPORT_XSLT);

        // Set up pre-processing chain to enable:
        // 1. Inclusions
        // 2. Abstract patterns
        // 3. SVRL report
        XsltTransformer stage1Transformer = includeXslt.load();
        XsltTransformer stage2Transformer = abstractXslt.load();
        XsltTransformer stage3Transformer = svrlXslt.load();

        stage1Transformer.setSource(schema);
        stage1Transformer.setDestination(stage2Transformer);
        stage2Transformer.setDestination(stage3Transformer);

        XdmDestination chainResult = new XdmDestination();
        stage3Transformer.setDestination(chainResult);
        if (null != phase && !phase.isEmpty()) {
            stage3Transformer.setParameter(new QName("phase"), new XdmAtomicValue(phase));
        }

        // redirect messages written to System.err by default message emitter
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        PrintStream console = System.err;
        try {
//            System.setErr(new PrintStream(baos));
            stage1Transformer.transform();
        } catch (SaxonApiException e) {
            throw new Exception(e.getMessage(), e.getCause());
//            throw new Exception(baos.toString() + e.getMessage(), e.getCause());
//        } finally {
//            System.setErr(console);
        }
        return compiler.compile(chainResult.getXdmNode().asSource());
    }

    public Result validate(Processor processor, XsltTransformer transformer, Source xmlSource, boolean svrlReport) {
        if (xmlSource == null) {
            throw new IllegalArgumentException("Nothing to validate.");
        }
        if (DOMSource.class.isInstance(xmlSource)) {
            // Saxon XsltTransformer will reject DOMSource wrapping an Element
            Node node = DOMSource.class.cast(xmlSource).getNode();
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Document doc = importElement((Element) node);
                xmlSource = new DOMSource(doc, xmlSource.getSystemId());
            }
        }
        int totalRuleViolations = 0;
        XdmDestination results = new XdmDestination();
        try {
            transformer.setSource(xmlSource);
            transformer.setDestination(results);
            transformer.transform();
        } catch (SaxonApiException e1) {
            LOG.warn(e1.getMessage());
        }
        totalRuleViolations = countRuleViolations(processor, results);
//        if (LOG.isLoggable(Level.FINER)) {
//            LOG.log(Level.FINER, "{0} Schematron rule violations found", totalRuleViolations);
//            writeResultsToTempFile(results);
//        }
        NodeInfo nodeInfo = results.getXdmNode().getUnderlyingNode();

        if (svrlReport) {
            return new DOMResult(NodeOverNodeInfo.wrap(nodeInfo));
        } else {
            return generateTextResult(processor, results.getXdmNode().asSource());
        }
    }

    StreamResult generateTextResult(Processor processor, Source svrlSource) {
        XsltCompiler compiler = processor.newXsltCompiler();
        StreamResult result = null;
        try {
            XsltExecutable exec = compiler.compile(new StreamSource(getClass().getResourceAsStream("svrl2text.xsl")));
            XsltTransformer transformer = exec.load();
            transformer.setSource(svrlSource);
            Serializer serializer = new Serializer();
            serializer.setOutputProperty(Serializer.Property.OMIT_XML_DECLARATION, "yes");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            serializer.setOutputStream(bos);
            transformer.setDestination(serializer);
            transformer.transform();
            result = new StreamResult(bos);
        } catch (SaxonApiException e) {
            LOG.warn(e.getMessage());
        }
        return result;
    }

    private int countRuleViolations(Processor processor, XdmDestination results) {
        XPathCompiler xpath = processor.newXPathCompiler();
        xpath.declareNamespace("svrl", ISO_SCHEMATRON_SVRL_NS);
        XdmAtomicValue totalCount = null;
        try {
            XPathExecutable exe = xpath.compile("count(//svrl:failed-assert) + count(//svrl:successful-report)");
            XPathSelector selector = exe.load();
            selector.setContextItem(results.getXdmNode());
            totalCount = (XdmAtomicValue) selector.evaluateSingle();
        } catch (SaxonApiException e) {
            LOG.warn(e.getMessage(), e);
        }
        return Integer.parseInt(totalCount.getValue().toString());
    }

    Document importElement(Element elem) {
        javax.xml.parsers.DocumentBuilder docBuilder = null;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            docBuilder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException ex) {
            LOG.warn(ex, ex);
        }
        Document newDoc = docBuilder.newDocument();
        Node newNode = newDoc.importNode(elem, true);
        newDoc.appendChild(newNode);
        return newDoc;
    }

    private String calcHash(StreamSource src) throws IOException, SAXException {

        MessageDigest digest = messageDigest();

        OutputStream out = new FakeOutputStream();
        DigestOutputStream digestStream = new DigestOutputStream(out, digest);

        try (InputStream is = src.getInputStream()) {
            IOUtils.copy(is, digestStream);
        }

        return digestHex(digest);
    }

    private String digestHex(MessageDigest digest) {
        return Hex.encodeHexString(digest.digest());
    }

    private MessageDigest messageDigest() throws IOException {
        try {
            return MessageDigest.getInstance(MessageDigestAlgorithms.SHA_512);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    class FakeOutputStream extends OutputStream {

        @Override
        public void write(int b) throws IOException {}

        @Override
        public void write(byte b[]) throws IOException {}

        @Override
        public void write(byte b[], int off, int len) throws IOException {}

        @Override
        public void flush() throws IOException {}

        @Override
        public void close() throws IOException {}
    }

    static class CachedItem {
        long lastAccessTime = System.currentTimeMillis();

        Processor processor;
        XsltExecutable xslt;

        CachedItem(Processor processor, XsltExecutable xslt) {
            this.processor = processor;
            this.xslt = xslt;
        }

        public void touch() {
            lastAccessTime = System.currentTimeMillis();
        }
    }
}
