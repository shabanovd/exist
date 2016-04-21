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
import org.exist.security.*;
import org.exist.security.internal.SecurityManagerImpl;
import org.exist.security.internal.aider.GroupAider;
import org.exist.security.internal.aider.UserAider;
import org.exist.storage.DBBroker;
import org.exist.storage.txn.Txn;
import org.exist.xmldb.XmldbURI;
import org.opensaml.DefaultBootstrap;
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
            //OpenSamlBootstrap.init();
            DefaultBootstrap.bootstrap();
            //generator = new SecureRandomIdentifierGenerator();
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
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
                    primaryGroup = executeAsSystemUser(new Unit<Group>() {
                        @Override
                        public Group execute(DBBroker broker) throws EXistException, PermissionDeniedException {
                            return addGroup(new GroupAider(ID, SAML));
                        }
                    });

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
        final Account account = getAccount(accountName);

        if(account == null) {
            throw new AuthenticationException(AuthenticationException.ACCOUNT_NOT_FOUND, "Account '" + accountName + "' not found.");
        }

        if(!account.isEnabled()) {
            throw new AuthenticationException(AuthenticationException.ACCOUNT_LOCKED, "Account '" + accountName + "' is disabled.");
        }

        throw new AuthenticationException(AuthenticationException.WRONG_PASSWORD, "Password base authentication can't be done for account [" + accountName + "] ");
    }

    @Override
    public boolean deleteAccount(Account account) throws PermissionDeniedException, EXistException {
        if(account == null) {
            return false;
        }

        usersByName.modify2E(principalDb -> {
            final AbstractAccount remove_account = (AbstractAccount)principalDb.get(account.getName());
            if(remove_account == null){
                throw new IllegalArgumentException("No such account exists!");
            }

            try (DBBroker broker = getDatabase().get(null)) {
                final Account user = broker.getSubject();

                if(!(account.getName().equals(user.getName()) || user.hasDbaRole()) ) {
                    throw new PermissionDeniedException("You are not allowed to delete '" +account.getName() + "' user");
                }

                LOG.info("delete account "+account+" by "+user.toString(), new Exception());

                remove_account.setRemoved(true);
                remove_account.setCollection(broker, collectionRemovedAccounts, XmldbURI.create(UUIDGenerator.getUUID()+".xml"));

                try (Txn txn = broker.beginTx()) {

                    collectionAccounts.removeXMLResource(txn, broker, XmldbURI.create( remove_account.getName() + ".xml"));

                    txn.success();

                } catch(final Exception e) {
                    LOG.debug("loading configuration failed: " + e.getMessage(), e);
                }

                getSecurityManager().addUser(remove_account.getId(), remove_account);
                principalDb.remove(remove_account.getName());
            } catch (Exception e) {
                LOG.debug(e.getMessage(), e);
            }
        });

        return true;
    }

    @Override
    public boolean deleteGroup(Group group) throws PermissionDeniedException, EXistException, ConfigurationException {
        return false;
    }

    protected Account createAccountInDatabase(final String username, final Map<SchemaType, String> metadata) throws AuthenticationException {

        try {
            return executeAsSystemUser(new Unit<Account>() {
                @Override
                public Account execute(DBBroker broker) throws EXistException, PermissionDeniedException {
                    // create the user account
                    UserAider userAider = new UserAider(ID, username, getPrimaryGroup());

                    // store any requested metadata
                    for (Entry<SchemaType, String> entry : metadata.entrySet())
                        userAider.setMetadataValue(entry.getKey(), entry.getValue());

                    Account account = getSecurityManager().addAccount(userAider);

                    return account;
                }
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
