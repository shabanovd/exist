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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.opensaml.common.SAMLException;
import org.opensaml.common.SAMLObject;
import org.opensaml.common.binding.SAMLMessageContext;
import org.opensaml.saml2.core.*;
import org.opensaml.xml.signature.SignatureValidator;
import org.opensaml.xml.validation.ValidationException;

import static org.exist.security.realm.saml.SAMLRealm.LOG;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
public class SAMLResponseVerifier {
    
    private SAMLResponseVerifier() {
    }

    public static void verify(SignatureValidator validator, SAMLMessageContext<Response, SAMLObject, NameID> samlMessageContext) throws SAMLException, ValidationException {
        Response samlResponse = samlMessageContext.getInboundSAMLMessage();

        //LOG.debug("SAML Response message : " + SAMLServlet.SAMLObjectToString(samlResponse));
        System.out.println(SAMLServlet.XMLToString(samlResponse.getDOM()));

        validator.validate(samlResponse.getSignature());

        Status status = samlResponse.getStatus();
        StatusCode statusCode = status.getStatusCode();
        String statusCodeURI = statusCode.getValue();
        if (!statusCodeURI.equals(StatusCode.SUCCESS_URI)) {
            LOG.warn("Incorrect SAML message code : " + statusCode.getStatusCode().getValue());
            throw new SAMLException("Incorrect SAML message code : " + statusCode.getValue());
        }
        if (samlResponse.getAssertions().size() == 0) {
            LOG.error("Response does not contain any acceptable assertions");
            throw new SAMLException("Response does not contain any acceptable assertions");
        }

        Assertion assertion = samlResponse.getAssertions().get(0);
        NameID nameId = assertion.getSubject().getNameID();
        if (nameId == null) {
            LOG.error("Name ID not present in subject");
            throw new SAMLException("Name ID not present in subject");
        }
        LOG.debug("SAML authenticated user " + nameId.getValue());
        verifyConditions(assertion.getConditions(), samlMessageContext);
        
        for (AttributeStatement attrStatement : assertion.getAttributeStatements()) {
            for (Attribute attr : attrStatement.getAttributes()) {
                System.out.println(attr.getName()+" = "+attr.getAttributeValues().get(0).getDOM().getTextContent());
                
            }
        }
    }

    private static void verifyConditions(Conditions conditions, SAMLMessageContext<Response, SAMLObject, NameID> samlMessageContext) throws SAMLException {
        verifyExpirationConditions(conditions);
        verifyAudienceRestrictions(conditions.getAudienceRestrictions(), samlMessageContext);
    }

    private static void verifyExpirationConditions(Conditions conditions) throws SAMLException {
        LOG.debug("Verifying conditions");

        DateTime currentTime = new DateTime(DateTimeZone.UTC);

        LOG.debug("Current time in UTC : " + currentTime);
        DateTime notBefore = conditions.getNotBefore();

        LOG.debug("Not before condition : " + notBefore);

        if ((notBefore != null) && currentTime.isBefore(notBefore))
            throw new SAMLException("Assertion is not conformed with notBefore condition");

        DateTime notOnOrAfter = conditions.getNotOnOrAfter();

        LOG.debug("Not on or after condition : " + notOnOrAfter);

        if ((notOnOrAfter != null) && currentTime.isAfter(notOnOrAfter))
            throw new SAMLException("Assertion is not conformed with notOnOrAfter condition");
    }

    private static void verifyAudienceRestrictions(List<AudienceRestriction> audienceRestrictions, SAMLMessageContext<Response, SAMLObject, NameID> samlMessageContext) throws SAMLException {
    }
}
