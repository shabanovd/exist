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

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerException;

import org.exist.util.serializer.DOMSerializer;
import org.opensaml.common.SAMLException;
import org.opensaml.common.SAMLObject;
import org.opensaml.common.binding.BasicSAMLMessageContext;
import org.opensaml.common.binding.SAMLMessageContext;
import org.opensaml.common.xml.SAMLConstants;
import org.opensaml.saml2.binding.decoding.HTTPPostDecoder;
import org.opensaml.saml2.core.NameID;
import org.opensaml.saml2.core.Response;
import org.opensaml.saml2.metadata.IDPSSODescriptor;
import org.opensaml.ws.security.SecurityPolicy;
import org.opensaml.ws.security.SecurityPolicyResolver;
import org.opensaml.ws.security.SecurityPolicyRule;
import org.opensaml.ws.security.provider.BasicSecurityPolicy;
import org.opensaml.ws.security.provider.HTTPRule;
import org.opensaml.ws.security.provider.MandatoryIssuerRule;
import org.opensaml.ws.security.provider.StaticSecurityPolicyResolver;
import org.opensaml.ws.transport.http.HttpServletRequestAdapter;
import org.opensaml.ws.transport.http.HttpServletResponseAdapter;
import org.opensaml.xml.XMLObject;
import org.opensaml.xml.io.Marshaller;
import org.opensaml.xml.util.XMLHelper;

import static org.exist.security.realm.saml.SAMLRealm.LOG;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
public class SAMLServlet extends HttpServlet {

    private static final long serialVersionUID = 7955176436954151916L;

    static final String SAML_RESPONSE = "SAMLResponse";
    static final String RELAY_STATE = "RelayState";

//    private static final String RETURN_TO_PAGE = "returnToPage";

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        process(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        process(request, response);
    }

    private void process(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (request.getPathInfo() == null)
            return;

        String path = request.getPathInfo().replace("/", "");

        if (LOG.isTraceEnabled()) {
            LOG.trace("the " + request.getMethod() + " method, path info " + path);
        }

        Service service;
        try {
            service = SAMLRealm.get().getServiceByPath(path);
        } catch (SAMLException e) {
            throw new ServletException(e);
        }

        // String relay = request.getParameter(RELAY_STATE);

        String responseMessage = request.getParameter(SAML_RESPONSE);

        if (responseMessage != null) {
            LOG.debug("Response from Identity Provider is received");
            try {
                LOG.debug("Decoding of SAML message");
                SAMLMessageContext<Response, SAMLObject, NameID> samlMessageContext = decodeSamlMessage(request, response);
                LOG.debug("SAML message has been decoded successfully");

                //samlMessageContext.setLocalEntityId(service.getSpProviderId());

                String relayState = "/";
                //XXX: getInitialRequestedResource(samlMessageContext);

                service.verify(samlMessageContext);

                LOG.debug("Starting and store SAML session..");
                service.createSAMLSession(request.getSession(), samlMessageContext);

                LOG.debug("User has been successfully authenticated in idP. Redirect to initial requested resource " + relayState);
                response.sendRedirect(relayState);
                return;
            } catch (Exception e) {
                throw new ServletException(e);
            }
        } else {
            service.sendAuthRequest(request, response);
            return;
        }

        // if (request.getParameterMap().containsKey(RETURN_TO_PAGE)) {
        //
        // String authorizationUrl = service.getAuthorizationUrl(EMPTY_TOKEN);
        //
        // request.getSession().setAttribute(RETURN_TO_PAGE,
        // request.getParameter(RETURN_TO_PAGE));
        //
        // response.sendRedirect(authorizationUrl);
        // return;
        // }
        //
        // try {
        // service.saveAccessToken(request, service, accessToken);
        // } catch (Exception e) {
        // throw new ServletException(e);
        // }

        // String returnToPage = (String)
        // request.getSession().getAttribute(RETURN_TO_PAGE);
        //
        // if (returnToPage != null) {
        // response.sendRedirect(returnToPage);
        // } else {
        // response.sendRedirect("/");
        // }
    }

    private SAMLMessageContext<Response, SAMLObject, NameID> decodeSamlMessage(HttpServletRequest request, HttpServletResponse response) throws Exception {

        SAMLMessageContext<Response, SAMLObject, NameID> samlMessageContext = new BasicSAMLMessageContext<Response, SAMLObject, NameID>();

        HttpServletRequestAdapter requestAdapter = new HttpServletRequestAdapter(request);
        samlMessageContext.setInboundMessageTransport(requestAdapter);
        samlMessageContext.setInboundSAMLProtocol(SAMLConstants.SAML20P_NS);

        HttpServletResponseAdapter responseAdapter = new HttpServletResponseAdapter(response, request.isSecure());
        samlMessageContext.setOutboundMessageTransport(responseAdapter);
        samlMessageContext.setPeerEntityRole(IDPSSODescriptor.DEFAULT_ELEMENT_NAME);

        SecurityPolicyResolver securityPolicyResolver = getSecurityPolicyResolver(request.isSecure());

        samlMessageContext.setSecurityPolicyResolver(securityPolicyResolver);
        HTTPPostDecoder samlMessageDecoder = new HTTPPostDecoder();
        samlMessageDecoder.decode(samlMessageContext);

        return samlMessageContext;
    }

    private SecurityPolicyResolver getSecurityPolicyResolver(boolean isSecured) {
        SecurityPolicy securityPolicy = new BasicSecurityPolicy();
        HTTPRule httpRule = new HTTPRule(null, null, isSecured);
        MandatoryIssuerRule mandatoryIssuerRule = new MandatoryIssuerRule();
        List<SecurityPolicyRule> securityPolicyRules = securityPolicy.getPolicyRules();
        securityPolicyRules.add(httpRule);
        securityPolicyRules.add(mandatoryIssuerRule);
        return new StaticSecurityPolicyResolver(securityPolicy);
    }

    public static String SAMLObjectToString(XMLObject samlObject) {
        try {
            Marshaller marshaller = org.opensaml.Configuration.getMarshallerFactory().getMarshaller(samlObject);
            org.w3c.dom.Element authDOM = marshaller.marshall(samlObject);
            StringWriter rspWrt = new StringWriter();
            XMLHelper.writeNode(authDOM, rspWrt);
            return rspWrt.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public static String XMLToString(org.w3c.dom.Element element) {
        
        Properties properties = new Properties();

        properties.setProperty(OutputKeys.ENCODING, "UTF-8");
        properties.setProperty(OutputKeys.INDENT, "true");
            
        try {
            StringWriter writer = new StringWriter();
            DOMSerializer serializer = new DOMSerializer(writer, properties);
            try {
                serializer.serialize(element);
            } catch (final TransformerException e) {
                //Nothing to do ?
            }
            return writer.toString();

//            return XMLHelper.prettyPrintXML(element);
//            StringWriter rspWrt = new StringWriter();
//            XMLHelper.writeNode(element, rspWrt);
//            return rspWrt.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
