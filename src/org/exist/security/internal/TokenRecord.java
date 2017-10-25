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
package org.exist.security.internal;

import org.exist.config.Configurable;
import org.exist.config.Configuration;
import org.exist.config.ConfigurationException;
import org.exist.config.Configurator;
import org.exist.config.annotation.ConfigurationClass;
import org.exist.config.annotation.ConfigurationFieldAsAttribute;
import org.exist.memtree.DocumentImpl;
import org.exist.memtree.MemTreeBuilder;
import org.xml.sax.helpers.AttributesImpl;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
@ConfigurationClass("record")
public class TokenRecord implements Configurable {

    @ConfigurationFieldAsAttribute("name") String name;

    @ConfigurationFieldAsAttribute("token") String token;

    @ConfigurationFieldAsAttribute("realm") String realm;
    @ConfigurationFieldAsAttribute("account") String account;

    @ConfigurationFieldAsAttribute("create") long created;
    @ConfigurationFieldAsAttribute("last-use") long lastUse;

    Configuration configuration;

    public TokenRecord() {
    }

    public TokenRecord(TokensManager tm, Configuration config) throws ConfigurationException {
        configuration = Configurator.configure(this, config);
    }

    public TokenRecord(TokenRecord o) {
        this.name = o.name;
        this.token = o.token;
        this.realm = o.realm;
        this.account = o.account;
        this.created = o.created;
        this.lastUse = o.lastUse;
    }

    public DocumentImpl toXml() {
        final MemTreeBuilder builder = new MemTreeBuilder();
        builder.startDocument();

        AttributesImpl attr = new AttributesImpl();
        attr.addAttribute("", "name", "name", "CDATA", name != null ? name : "");
        attr.addAttribute("", "token", "token", "CDATA", token);
        attr.addAttribute("", "realm", "realm", "CDATA", realm);
        attr.addAttribute("", "account", "account", "CDATA", account);
        attr.addAttribute("", "created", "created", "CDATA", String.valueOf(created));
        attr.addAttribute("", "last-use", "last-use", "CDATA", String.valueOf(lastUse));

        builder.startElement("", "record", "record", attr);
        builder.endElement();

        builder.endDocument();

        return builder.getDocument();
    }

    public String name() {
        return name;
    }

    public String token() {
        return token;
    }

    public String realm() {
        return realm;
    }

    public String account() {
        return account;
    }

    public long created() {
        return created;
    }

    public long lastUse() {
        return lastUse;
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
