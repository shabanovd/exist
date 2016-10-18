/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2011 The eXist Project
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
package org.exist.security.realm.saml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.Subject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.codec.binary.Base64;
import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.UserIdentity;
import org.exist.EXistException;
import org.exist.config.Configurable;
import org.exist.config.Configuration;
import org.exist.config.Configurator;
import org.exist.config.annotation.ConfigurationClass;
import org.exist.config.annotation.ConfigurationFieldAsAttribute;
import org.exist.config.annotation.ConfigurationFieldAsElement;
import org.exist.config.annotation.ConfigurationFieldClassMask;
import org.exist.security.*;
import org.exist.security.internal.HttpSessionAuthentication;
import org.exist.security.internal.SubjectAccreditedImpl;
import org.opensaml.common.SAMLException;
import org.opensaml.common.SAMLObject;
import org.opensaml.common.binding.SAMLMessageContext;
import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Attribute;
import org.opensaml.saml2.core.AttributeStatement;
import org.opensaml.saml2.core.AuthnStatement;
import org.opensaml.saml2.core.NameID;
import org.opensaml.saml2.core.Response;
import org.opensaml.xml.signature.SignatureValidator;
import org.opensaml.xml.validation.ValidationException;

/**
 * <service name="app">
 *      <signCertificate>...</signCertificate>
 * </service>
 * 
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
@ConfigurationClass("service")
public class Service implements Configurable {
    
    public static String SAML_SESSION_INFO = "SAML_SESSION_INFO";

    private SAMLRealm realm = null;
    private Configuration configuration = null;

    @ConfigurationFieldAsAttribute("set-group")
    String set_group;

    @ConfigurationFieldAsAttribute("force-lowercase")
    boolean forceLowercase = false;

    @ConfigurationFieldAsAttribute("name")
    String name;

    @ConfigurationFieldAsElement("account-id-by-attribute")
    String accountIdAttribute;

    @ConfigurationFieldAsElement("spID")
    String spId;

    @ConfigurationFieldAsElement("auth-method")
    String auth_method = "redirect";

    @ConfigurationFieldAsElement("auth-url")
    String auth_url;

    @ConfigurationFieldAsElement("return-url")
    String return_url;
    
    @ConfigurationFieldAsElement("signCertificate")
    String signCertificate;

    @ConfigurationFieldAsElement("attribute-to-schema-type")
    @ConfigurationFieldClassMask("org.exist.security.realm.saml.SchemaTypeMapping")
    private List<SchemaTypeMapping> mapAttributes = new ArrayList<>();

    Certificate certSigning;
    
    public Service(SAMLRealm realm, Configuration config) {

        this.realm = realm;

        configuration = Configurator.configure(this, config);
    }

    public String getName() {
        return name;
    }

    public void sendAuthRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if ("redirect".equals(auth_method)) {
            response.sendRedirect(getAuthURL());

        } else if ("post".equals(auth_method)) {
            SAMLRequestSender samlRequestSender = new SAMLRequestSender();

          try {
            samlRequestSender.sendSAMLAuthRequest(request, response,
                spId, getReturnURL(), getAuthURL());
          } catch (Exception e) {
            SAMLRealm.LOG.error(e.getMessage(), e);
            throw new ServletException(e);
          }
        } else {
            SAMLRealm.LOG.error("unknown authentication method '"+auth_method+"'");
            throw new ServletException("unknown authentication method '"+auth_method+"'");
        }
    }

    public String getAuthURL() {
        return auth_url;
    }

    public String getReturnURL() {
        return return_url;
    }
    
    public void setSignCertificate(String certData) throws CertificateException {
        
        signCertificate = certData;

        byte[] certbytes = Base64.decodeBase64(certData);
        
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate x509Cert = (X509Certificate)cf.generateCertificate(new ByteArrayInputStream(certbytes));
    
            certSigning = new Certificate(x509Cert);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void verify(SAMLMessageContext<Response, SAMLObject, NameID> samlMessageContext) throws PermissionDeniedException {
        SignatureValidator validator = new SignatureValidator(certSigning.getCredential());
        
        try {
            SAMLResponseVerifier.verify(validator, samlMessageContext);
        } catch (SAMLException | ValidationException e) {
            throw new PermissionDeniedException(e.getMessage(), e);
        }
    }
    
    public void createSAMLSession(HttpSession session, SAMLMessageContext<Response, SAMLObject, NameID> samlMessageContext) throws AuthenticationException {
        List<Assertion> assertions = samlMessageContext.getInboundSAMLMessage().getAssertions();
        
        NameID nameId = (assertions.size() != 0 && assertions.get(0).getSubject() != null) ? assertions.get(0).getSubject().getNameID() : null;
        String nameValue = nameId == null ? null : nameId.getValue();
        
        if (nameValue == null) {
            throw new AuthenticationException(AuthenticationException.UNNOWN_EXCEPTION, "can't get id at SAML response");
        }
        
        Map<String, String> responseAttributes = getAttributesMap(getSAMLAttributes(assertions));

        if (accountIdAttribute != null) {
            nameValue = responseAttributes.get(accountIdAttribute);

            if (nameValue == null) {
                throw new AuthenticationException(AuthenticationException.UNNOWN_EXCEPTION, "attribute '"+accountIdAttribute+"' is null and can't be used as account id");
            }

            nameValue = nameValue.trim();

            if (nameValue.isEmpty()) {
                throw new AuthenticationException(AuthenticationException.UNNOWN_EXCEPTION, "attribute '"+accountIdAttribute+"' is empty and can't be used as account id");
            }
        }

        String accountName = forceLowercase ? nameValue.toLowerCase() : nameValue;

        Account found = SAMLRealm.get().getSecurityManager().getAccount(accountName);
        
        if (found == null) {
            Map<SchemaType, String> metadata = new HashMap<>();
//            addMetadata(responseAttributes, metadata, AXSchemaType.ID, "id");
            addMetadata(responseAttributes, metadata, AXSchemaType.FIRSTNAME, "FirstName");
            addMetadata(responseAttributes, metadata, AXSchemaType.LASTNAME, "LastName");
            
            addMetadata(responseAttributes, metadata, AXSchemaType.FULLNAME, "name");
//            addMetadata(responseAttributes, metadata, AXSchemaType.TIMEZONE, "timezone");
//
//            addMetadata(responseAttributes, metadata, GoogleSchemaType.PICTURE, "picture");
//            addMetadata(responseAttributes, metadata, GoogleSchemaType.LOCALE, "locale");
//            addMetadata(responseAttributes, metadata, GoogleSchemaType.LINK, "link");
//            addMetadata(responseAttributes, metadata, GoogleSchemaType.GENDER, "gender");                   
            
            found = SAMLRealm.get().createAccountInDatabase(accountName, metadata);
        }

        if (set_group != null) {

            String[] groups = set_group.split(",");

            boolean doUpdate = false;
            for (String group : groups) {
                if (!found.hasGroup(group)) {
                    doUpdate = true;
                    break;
                }
            }

            if (doUpdate) {

                final Account account = found;

                try {
                    realm.executeAsSystemUser(broker -> {
                        for (String group : groups) {
                            if (!account.hasGroup(group)) {
                                account.addGroup(group);
                            }
                        }

                        account.save();

                        return account;
                    });
                } catch (EXistException | PermissionDeniedException e) {
                    throw new AuthenticationException(
                        AuthenticationException.UNNOWN_EXCEPTION,
                        "can't add account to group '" + set_group + "'",
                        e
                    );
                }
            }
        }

        if (!mapAttributes.isEmpty()) {
            final Account account = found;
            try {
                realm.executeAsSystemUser(broker -> {
                    mapAttributes.forEach(mapping -> {
                        String val = responseAttributes.get(mapping.getAttribute());

                        if (val != null) {
                            account.setMetadataValue(mapping, val);
                        }
                    });

                    account.save();

                    return account;
                });
            } catch (EXistException | PermissionDeniedException e) {
                throw new AuthenticationException(
                    AuthenticationException.UNNOWN_EXCEPTION,
                    "can't update account's metadata",
                    e
                );
            }
        }

        Account principal = new SubjectAccreditedImpl((AbstractAccount) found, assertions, getSAMLSessionValidTo(assertions));
        
        Subject subject = new Subject();

        //TODO: hardcoded to jetty - rewrite
        //*******************************************************
        DefaultIdentityService _identityService = new DefaultIdentityService();
        UserIdentity user = _identityService.newUserIdentity(subject, principal, new String[0]);

        Authentication cached=new HttpSessionAuthentication(session, user);
        session.setAttribute(HttpSessionAuthentication.__J_AUTHENTICATED, cached);
        //*******************************************************
                
        session.setAttribute(SAML_SESSION_INFO, assertions);
    }
    
    private static void addMetadata(Map<String, String> attributes, Map<SchemaType, String> metadata, SchemaType type, String attrName) {
        String val = attributes.get(attrName);

        if (val != null) {
            metadata.put(type, val);
        }
    }

    public void destroySAMLSession(HttpSession session) {
        session.removeAttribute(SAML_SESSION_INFO);
    }

    public List<Attribute> getSAMLAttributes(List<Assertion> assertions) {
        List<Attribute> attributes = new ArrayList<>();
        if (assertions != null) {
            for (Assertion assertion : assertions) {
                for (AttributeStatement attributeStatement : assertion.getAttributeStatements()) {
                    for (Attribute attribute : attributeStatement.getAttributes()) {
                        attributes.add(attribute);
                    }
                }
            }
        }
        return attributes;
    }

    public Date getSAMLSessionValidTo(List<Assertion> assertions) {
        org.joda.time.DateTime sessionNotOnOrAfter = null;
        if (assertions != null) {
            for (Assertion assertion : assertions) {
                for (AuthnStatement statement : assertion.getAuthnStatements()) {
                    sessionNotOnOrAfter = statement.getSessionNotOnOrAfter();
                }
            }
        }

        return sessionNotOnOrAfter != null ? sessionNotOnOrAfter.toCalendar(Locale.getDefault()).getTime() : null;
    }

    public Map<String, String> getAttributesMap(List<Attribute> attributes) {
        Map<String, String> result = new HashMap<>();
        for (Attribute attribute : attributes) {
            result.put(attribute.getName(), attribute.getDOM().getTextContent());
        }
        return result;
    }

    public boolean isConfigured() {
        return configuration != null;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}