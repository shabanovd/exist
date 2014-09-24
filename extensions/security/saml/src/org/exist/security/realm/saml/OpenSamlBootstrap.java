/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2014 The eXist Project
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
 */
package org.exist.security.realm.saml;

import org.opensaml.DefaultBootstrap;
import org.opensaml.xml.ConfigurationException;

import static org.exist.security.realm.saml.SAMLRealm.LOG;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
public class OpenSamlBootstrap extends DefaultBootstrap {

    private static boolean initialized;
    private static String[] xmlToolingConfigs = { 
        "/default-config.xml", 
        "/encryption-validation-config.xml", 
        "/saml2-assertion-config.xml", 
        "/saml2-assertion-delegation-restriction-config.xml", 
        "/saml2-core-validation-config.xml",
        "/saml2-metadata-config.xml", 
        "/saml2-metadata-idp-discovery-config.xml", 
        "/saml2-metadata-query-config.xml", 
        "/saml2-metadata-validation-config.xml", 
        "/saml2-protocol-config.xml", 
        "/saml2-protocol-thirdparty-config.xml",
        "/schema-config.xml", 
        "/signature-config.xml", 
        "/signature-validation-config.xml" 
    };

    public static synchronized void init() {
        if (!initialized) {
            try {
                initializeXMLTooling(xmlToolingConfigs);
            } catch (ConfigurationException e) {
                LOG.error("Unable to initialize opensaml DefaultBootstrap", e);
            }
            initializeGlobalSecurityConfiguration();
            initialized = true;
        }
    }
}
