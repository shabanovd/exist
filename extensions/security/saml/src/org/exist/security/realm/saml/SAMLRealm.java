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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.exist.EXistException;
import org.exist.config.Configuration;
import org.exist.config.ConfigurationException;
import org.exist.config.Configurator;
import org.exist.config.annotation.*;
import org.exist.security.AbstractRealm;
import org.exist.security.Account;
import org.exist.security.AuthenticationException;
import org.exist.security.Group;
import org.exist.security.PermissionDeniedException;
import org.exist.security.SchemaType;
import org.exist.security.Subject;
import org.exist.security.internal.SecurityManagerImpl;
import org.exist.security.internal.aider.GroupAider;
import org.exist.security.internal.aider.UserAider;
import org.exist.storage.DBBroker;
import org.opensaml.common.SAMLException;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
@ConfigurationClass("realm")
public class SAMLRealm extends AbstractRealm {

    protected final static Logger LOG = Logger.getLogger(SAMLRealm.class);
    private final static String SAML = "SAML";

    protected static SAMLRealm instance = null;

    public static SAMLRealm get() {
        return instance;
    }

    @ConfigurationFieldAsAttribute("id")
    public final static String ID = "SAML";

    @ConfigurationFieldAsAttribute("version")
    public final static String version = "1.0";

    // @ConfigurationReferenceBy("name")
    @ConfigurationFieldAsElement("service")
    @ConfigurationFieldClassMask("org.exist.security.realm.saml.Service")
    List<Service> services;

    private Group primaryGroup = null;

    public SAMLRealm(final SecurityManagerImpl sm, Configuration config) throws ConfigurationException {
        super(sm, config);
        instance = this;

        configuration = Configurator.configure(this, config);

        try {
            OpenSamlBootstrap.init();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Override
    public String getId() {
        return ID;
    }

    private synchronized Group getPrimaryGroup() throws PermissionDeniedException {
        if (primaryGroup == null) {
            primaryGroup = getGroup(SAML);
            if (primaryGroup == null)
                try {
                    primaryGroup = executeAsSystemUser(broker -> addGroup(new GroupAider(ID, SAML)));

                    if (primaryGroup == null)
                        throw new ConfigurationException("SAML realm can not create primary group 'SAML'.");

                } catch (PermissionDeniedException e) {
                    throw e;
                } catch (EXistException e) {
                    throw new PermissionDeniedException(e);
                }
        }
        return primaryGroup;
    }

    @Override
    public Subject authenticate(final String accountName, Object credentials) throws AuthenticationException {
        return null;
    }

    @Override
    public boolean deleteAccount(Account account) throws PermissionDeniedException, EXistException {
        return false;
    }

    @Override
    public boolean deleteGroup(Group group) throws PermissionDeniedException, EXistException {
        return false;
    }

    protected Account createAccountInDatabase(final String username, final Map<SchemaType, String> metadata) throws AuthenticationException {

        try {
            return executeAsSystemUser(broker -> {
                // create the user account
                UserAider userAider = new UserAider(ID, username, getPrimaryGroup());

                // store any requested metadata
                for (Entry<SchemaType, String> entry : metadata.entrySet())
                    userAider.setMetadataValue(entry.getKey(), entry.getValue());

                Account account = getSecurityManager().addAccount(userAider);

                return account;
            });
        } catch (Exception e) {
            throw new AuthenticationException(AuthenticationException.UNNOWN_EXCEPTION, e.getMessage(), e);
        }
    }

    public Service getServiceByPath(String name) throws SAMLException {
        for (Service service : services) {
            if (service.getName().equals(name)) {
                return service;
            }
        }
        //XXX: replace by NotFoundException
        throw new SAMLException("Service not found by name '" + name + "'.");
    }
}
