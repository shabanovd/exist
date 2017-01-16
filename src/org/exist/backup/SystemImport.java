/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2012 The eXist Project
 *  http://exist-db.org
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 *  $Id$
 */
package org.exist.backup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.Database;
import org.exist.backup.restore.SystemImportHandler;
import org.exist.backup.restore.listener.RestoreListener;
import org.exist.config.ConfigurationException;
import org.exist.security.AuthenticationException;
import org.exist.security.PermissionDeniedException;
import org.exist.security.Subject;
import org.exist.storage.DBBroker;
import org.exist.util.EXistInputSource;
import org.exist.xmldb.XmldbURI;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xmldb.api.base.ErrorCodes;
import org.xmldb.api.base.XMLDBException;

/**
 * Restore 
 *
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class SystemImport {
    
    public final static Logger LOG = LogManager.getLogger( SystemImport.class );

    private static AtomicInteger processing = new AtomicInteger();
    public static boolean isRunning() {
        return processing.get() == 0;
    }

    private Database db;
    
    public SystemImport(Database db) {
    	this.db = db;
	}

    public void restore(RestoreListener listener, String username, Object credentials, String newCredentials, File f, String uri) throws XMLDBException, FileNotFoundException, IOException, SAXException, ParserConfigurationException, URISyntaxException, AuthenticationException, ConfigurationException, PermissionDeniedException {
        
        //login
        final DBBroker broker = db.authenticate(username, credentials);
        processing.incrementAndGet();
        try {
            //set the new password
            if (newCredentials != null) {
                setAdminCredentials(broker, newCredentials);
            }

            //restore /db/system
            restore(listener, broker, getSystemBackupDescriptors(f), uri, false);

            //get the backup descriptors, can be more than one if it was an incremental backup
            restore(listener, broker, getBackupDescriptors(f), uri, true);
        } finally {
            processing.decrementAndGet();
            db.release(broker);
        }
    }

    private void restore(final RestoreListener listener, final DBBroker broker, final Stack<BackupDescriptor> descriptors, String uri, boolean disableTriggers) throws XMLDBException, FileNotFoundException, IOException, SAXException, ParserConfigurationException, URISyntaxException, AuthenticationException, ConfigurationException, PermissionDeniedException {

        if (disableTriggers) {
            broker.disableTriggers();
        }
        try {
            final SAXParserFactory saxFactory = SAXParserFactory.newInstance();
            saxFactory.setNamespaceAware(true);
            saxFactory.setValidating(false);
            final SAXParser sax = saxFactory.newSAXParser();
            final XMLReader reader = sax.getXMLReader();

            try {
                listener.restoreStarting();

                while(!descriptors.isEmpty()) {
                    final BackupDescriptor descriptor = descriptors.pop();
                    final EXistInputSource is = descriptor.getInputSource();
                    is.setEncoding( "UTF-8" );

                    final SystemImportHandler handler = new SystemImportHandler(broker, listener, uri, descriptor);

                    reader.setContentHandler(handler);
                    reader.parse(is);
                }
            } finally {
                listener.restoreFinished();
            }
        } finally {
            if (disableTriggers) {
                broker.enableTriggers();
            }
        }
    }
    
    private static Stack<BackupDescriptor> getSystemBackupDescriptors(File contents) throws XMLDBException, IOException {
        
        final Stack<BackupDescriptor> descriptors = new Stack<>();
        
        final BackupDescriptor bd = getBackupDescriptor(contents);

        // check if the system collection is in the backup. This should be processed first
        final BackupDescriptor sysDescriptor = bd.getChildBackupDescriptor(XmldbURI.SYSTEM_COLLECTION_NAME);

        // check if the system/security collection is in the backup, this must be the first system collection processed
        if(sysDescriptor != null) {
            descriptors.push(sysDescriptor);

            final BackupDescriptor secDescriptor = sysDescriptor.getChildBackupDescriptor("security");
            if(secDescriptor != null) {
                descriptors.push(secDescriptor);
            }
        }

        return descriptors;
    }

    public static Stack<BackupDescriptor> getBackupDescriptors(File contents) throws XMLDBException, IOException {

        final Stack<BackupDescriptor> descriptors = new Stack<>();

        do {
            final BackupDescriptor bd = getBackupDescriptor(contents);
            descriptors.push(bd);

            contents = null;

            final Properties properties = bd.getProperties();
            if((properties != null ) && "yes".equals(properties.getProperty("incremental", "no"))) {
                final String previous = properties.getProperty("previous", "");

                if(previous.length() > 0) {
                    contents = new File(bd.getParentDir(), previous);

                    if(!contents.canRead()) {
                        throw new XMLDBException(ErrorCodes.PERMISSION_DENIED, "Required part of incremental backup not found: " + contents.getAbsolutePath());
                    }
                }
            }
        } while(contents != null);

        return descriptors;
    }
    
    private static BackupDescriptor getBackupDescriptor(File f) throws IOException {
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
    
    private void setAdminCredentials(DBBroker broker, String newCredentials) throws ConfigurationException, PermissionDeniedException {
    	final Subject subject = broker.getSubject();
    	subject.setPassword(newCredentials);
    	subject.save(broker);
    }
}