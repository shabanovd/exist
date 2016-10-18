/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2016 The eXist Project
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.joda.time.DateTime;
import org.opensaml.common.SAMLObject;
import org.opensaml.common.SAMLVersion;
import org.opensaml.common.xml.SAMLConstants;
import org.opensaml.saml2.binding.encoding.HTTPRedirectDeflateEncoder;
import org.opensaml.saml2.core.AuthnRequest;
import org.opensaml.saml2.core.Issuer;
import org.opensaml.saml2.core.impl.AuthnRequestBuilder;
import org.opensaml.saml2.core.impl.IssuerBuilder;
import org.opensaml.util.URLBuilder;
import org.opensaml.ws.message.encoder.MessageEncodingException;
import org.opensaml.ws.transport.http.HTTPTransportUtils;
import org.opensaml.ws.transport.http.HttpServletResponseAdapter;
import org.opensaml.xml.util.Pair;

//import static org.exist.security.realm.saml.SAMLRealm.LOG;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
public class SAMLRequestSender {

    private SAMLAuthnRequestBuilder samlAuthnRequestBuilder = new SAMLAuthnRequestBuilder();
    private MessageEncoder messageEncoder = new MessageEncoder();

    public void sendSAMLAuthRequest(HttpServletRequest request, HttpServletResponse servletResponse, String spId, String acsUrl, String idpUrl) throws Exception {
        AuthnRequest authnRequest = samlAuthnRequestBuilder.buildRequest(spId, acsUrl, idpUrl);
        // store SAML 2.0 authentication request
        //String key = SAMLRequestStore.getInstance().storeRequest();
        //authnRequest.setID(key);

        if (SAMLRealm.LOG.isDebugEnabled()) {
          SAMLRealm.LOG.debug("SAML Authentication message : " + SAMLServlet.SAMLObjectToString(authnRequest));
        }

        String redirectURL = messageEncoder.encode(authnRequest, idpUrl, request.getRequestURI());

        HttpServletResponseAdapter responseAdapter = new HttpServletResponseAdapter(servletResponse, request.isSecure());
        HTTPTransportUtils.addNoCacheHeaders(responseAdapter);
        HTTPTransportUtils.setUTF8Encoding(responseAdapter);
        responseAdapter.sendRedirect(redirectURL);

    }

    private static class SAMLAuthnRequestBuilder {

        public AuthnRequest buildRequest(String spProviderId, String acsUrl, String idpUrl) {
            /* Building Issuer object */
            IssuerBuilder issuerBuilder = new IssuerBuilder();
            Issuer issuer = issuerBuilder.buildObject("urn:oasis:names:tc:SAML:2.0:assertion", "Issuer", "saml2p");
            issuer.setValue(spProviderId);

            /* Creation of AuthRequestObject */
            DateTime issueInstant = new DateTime();
            AuthnRequestBuilder authRequestBuilder = new AuthnRequestBuilder();

            AuthnRequest authRequest = authRequestBuilder.buildObject(SAMLConstants.SAML20P_NS, "AuthnRequest", "saml2p");
            authRequest.setForceAuthn(false);
            authRequest.setIssueInstant(issueInstant);
            authRequest.setProtocolBinding(SAMLConstants.SAML2_POST_BINDING_URI);
            authRequest.setAssertionConsumerServiceURL(acsUrl);
            authRequest.setIssuer(issuer);
            //XXX: authRequest.setNameIDPolicy(nameIdPolicy); //urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress
            authRequest.setVersion(SAMLVersion.VERSION_20);
            authRequest.setDestination(idpUrl);

            return authRequest;
        }
    }

    private static class MessageEncoder extends HTTPRedirectDeflateEncoder {
        public String encode(SAMLObject message, String endpointURL, String relayState) throws MessageEncodingException {
            String encodedMessage = deflateAndBase64Encode(message);
            return buildRedirectURL(endpointURL, relayState, encodedMessage);
        }

        public String buildRedirectURL(String endpointURL, String relayState, String message) throws MessageEncodingException {
            URLBuilder urlBuilder = new URLBuilder(endpointURL);
            List<Pair<String, String>> queryParams = urlBuilder.getQueryParams();
            queryParams.clear();
            queryParams.add(new Pair<>("SAMLRequest", message));
            if (checkRelayState(relayState)) {
                queryParams.add(new Pair<>("RelayState", relayState));
            }
            return urlBuilder.buildURL();
        }
    }
}
