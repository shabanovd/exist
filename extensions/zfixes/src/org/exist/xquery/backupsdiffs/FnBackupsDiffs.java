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
package org.exist.xquery.backupsdiffs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.backup.BackupDescriptor;
import org.exist.backup.FileSystemBackupDescriptor;
import org.exist.backup.ZipArchiveBackupDescriptor;
import org.exist.backup.restore.listener.RestoreListener;
import org.exist.config.ConfigurationException;
import org.exist.dom.QName;
import org.exist.memtree.MemTreeBuilder;
import org.exist.security.AuthenticationException;
import org.exist.security.PermissionDeniedException;
import org.exist.util.EXistInputSource;
import org.exist.xquery.*;
import org.exist.xquery.fixes.Module;
import org.exist.xquery.functions.system.SystemModule;
import org.exist.xquery.restore.AbstractRestoreListener;
import org.exist.xquery.restore.SystemImport;
import org.exist.xquery.value.*;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xmldb.api.base.XMLDBException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FnBackupsDiffs extends BasicFunction {

    protected final static Logger LOG = LogManager.getLogger(FnBackupsDiffs.class);

    protected final static QName NAME =
            new QName("backups-diffs", Module.NAMESPACE_URI, Module.PREFIX);

    protected final static String DESCRIPTION =
        "Show differences for backups";

    protected final static FunctionParameterSequenceType DIRorFILE =
        new FunctionParameterSequenceType("dir-or-file", Type.STRING, Cardinality.EXACTLY_ONE,
                "This is either a backup directory with the backup descriptor (__contents__.xml) or a backup ZIP file.");

    protected final static FunctionReturnSequenceType RETURN =
        new FunctionReturnSequenceType(Type.NODE, Cardinality.EXACTLY_ONE, "the backups differences");

    public final static FunctionSignature signatures[] = {
        new FunctionSignature(
            NAME,
            DESCRIPTION,
            new SequenceType[] {
                DIRorFILE,
                DIRorFILE
            },
            RETURN
        )
    };
    private static final QName REPORT_ELEMENT = NAME;

    public FnBackupsDiffs(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }

    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        if( !context.getSubject().hasDbaRole() )
            {throw( new XPathException( this, "Permission denied, calling user '" + context.getSubject().getName() + "' must be a DBA" ) );}

        final String backupOne = args[0].getStringValue();
        final String backupTwo = args[1].getStringValue();

        Tasks manager = new Tasks();

        MemTreeBuilder builder = context.getDocumentBuilder();
        builder.startDocument();
        builder.startElement(REPORT_ELEMENT, null);

        XMLRestoreListener listener = new XMLRestoreListener(builder);

        try {

            BackupDescriptor bOne = getBackupDescriptor(new File(backupOne));
            BackupDescriptor bTwo = getBackupDescriptor(new File(backupTwo));

            run(manager, listener, bOne, bTwo);

        } catch (final Exception e) {
            e.printStackTrace();
            throw new XPathException(this, "restore failed with exception: " + e.getMessage(), e);
        }

        try {
            manager.waitTillDone();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        builder.endElement();
        builder.endDocument();
        return (NodeValue) builder.getDocument().getDocumentElement();
    }

    private void run(Tasks tasks, RestoreListener listener, BackupDescriptor bOne, BackupDescriptor bTwo) throws XMLDBException, IOException, SAXException, ParserConfigurationException, URISyntaxException, AuthenticationException, ConfigurationException, PermissionDeniedException {

        final SAXParserFactory saxFactory = SAXParserFactory.newInstance();
        saxFactory.setNamespaceAware(true);
        saxFactory.setValidating(false);
        final SAXParser sax = saxFactory.newSAXParser();
        final XMLReader reader = sax.getXMLReader();

        try {
            listener.restoreStarting();

            EXistInputSource isOne = bOne.getInputSource();
            isOne.setEncoding( "UTF-8" );

            BackupsDiffsHandler hOne = new BackupsDiffsHandler(listener, bOne);

            reader.setContentHandler(hOne);
            reader.parse(isOne);

            EXistInputSource isTwo = bTwo.getInputSource();
            isTwo.setEncoding( "UTF-8" );

            BackupsDiffsHandler hTwo = new BackupsDiffsHandler(listener, bTwo);

            reader.setContentHandler(hTwo);
            reader.parse(isTwo);

            hOne.diffs(tasks, hTwo);

        } finally {
            listener.restoreFinished();
        }
    }

    private BackupDescriptor getBackupDescriptor(File f) throws IOException {
        final BackupDescriptor bd;
        if(f.isDirectory()) {
            bd = new FileSystemBackupDescriptor(new File(new File(f, "db"), BackupDescriptor.COLLECTION_DESCRIPTOR));
        } else if(f.getName().toLowerCase().endsWith( ".zip" )) {
            bd = new ZipArchiveBackupDescriptor(f);
        } else {
            bd = new FileSystemBackupDescriptor(f);
        }
        return bd;
    }

    private static class XMLRestoreListener extends AbstractRestoreListener {

        public final static QName COLLECTION_ELEMENT = new QName("collection", SystemModule.NAMESPACE_URI, SystemModule.PREFIX);
        public final static QName RESOURCE_ELEMENT = new QName("resource", SystemModule.NAMESPACE_URI, SystemModule.PREFIX);
        public final static QName INFO_ELEMENT = new QName("info", SystemModule.NAMESPACE_URI, SystemModule.PREFIX);
        public final static QName WARN_ELEMENT = new QName("warn", SystemModule.NAMESPACE_URI, SystemModule.PREFIX);
        public final static QName ERROR_ELEMENT = new QName("error", SystemModule.NAMESPACE_URI, SystemModule.PREFIX);

        private final MemTreeBuilder builder;

        private XMLRestoreListener(MemTreeBuilder builder) {
            this.builder = builder;
        }

        @Override
        public void restoreStarting() {
            info("Starting diffs processing of backups...");
        }

        @Override
        public void restoreFinished() {
            info("Finished diffs processing of backups.");
        }

        @Override
        public void setCurrentBackup(String currentBackup) {
        }

        @Override
        public void createCollection(String collection) {
//            if (builder == null) {
//                LOG.info("Create collection "+collection);
//            } else {
//                builder.startElement(COLLECTION_ELEMENT, null);
//                builder.characters(collection);
//                builder.endElement();
//            }
        }

        @Override
        public void restored(String resource) {
//            if (builder == null) {
//                LOG.info("Restore resource "+resource);
//            } else {
//                builder.startElement(RESOURCE_ELEMENT, null);
//                builder.characters(resource);
//                builder.endElement();
//            }
        }

        @Override
        public void info(String message) {
            if (builder == null) {
                LOG.info(message);
            } else {
                builder.startElement(INFO_ELEMENT, null);
                builder.characters(message);
                builder.endElement();
            }
        }

        @Override
        public void warn(String message) {
            super.warn(message);

            if (builder == null) {
                LOG.warn(message);
            } else {
                builder.startElement(WARN_ELEMENT, null);
                builder.characters(message);
                builder.endElement();
            }
        }

        @Override
        public void error(String message) {
            super.error(message);

            if (builder == null) {
                LOG.error(message);
            } else {
                LOG.error(message);

                builder.startElement(ERROR_ELEMENT, null);
                builder.characters(message);
                builder.endElement();
            }
        }
    }
}
