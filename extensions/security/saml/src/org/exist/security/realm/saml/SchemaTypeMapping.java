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
package org.exist.security.realm.saml;

import org.exist.config.Configurable;
import org.exist.config.Configuration;
import org.exist.config.ConfigurationException;
import org.exist.config.Configurator;
import org.exist.config.annotation.ConfigurationClass;
import org.exist.config.annotation.ConfigurationFieldAsAttribute;
import org.exist.security.SchemaType;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
@ConfigurationClass("attribute-to-schema-type")
public class SchemaTypeMapping implements SchemaType, Configurable {

    @ConfigurationFieldAsAttribute("namespace")
    String namespace;

    @ConfigurationFieldAsAttribute("alias")
    String alias;

    @ConfigurationFieldAsAttribute("attribute")
    String attribute;

    Configuration configuration;

    public SchemaTypeMapping() {
    }

    public SchemaTypeMapping(Configuration config) throws ConfigurationException {
        configuration = Configurator.configure(this, config);
    }

    @Override
    public String getNamespace() {
        return namespace;
    }

    @Override
    public String getAlias() {
        return alias;
    }

    public String getAttribute() {
        return attribute;
    }

    @Override
    public boolean isConfigured() {
        return configuration != null;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }
}
