/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-06 Wolfgang M. Meier
 *  wolfgang@exist-db.org
 *  http://exist.sourceforge.net
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
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *  $Id$
 */
package org.exist.http.servlets;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.xml.transform.OutputKeys;

import org.apache.log4j.Logger;
import org.exist.EXistException;
import org.exist.http.Descriptor;
import org.exist.security.AuthenticationException;
import org.exist.security.PermissionDeniedException;
import org.exist.security.Subject;
import org.exist.security.internal.web.HttpAccount;
import org.exist.security.xacml.AccessContext;
import org.exist.source.FileSource;
import org.exist.source.Source;
import org.exist.source.SourceFactory;
import org.exist.storage.DBBroker;
import org.exist.storage.serializers.Serializer;
import org.exist.util.MimeTable;
import org.exist.util.serializer.SAXSerializer;
import org.exist.util.serializer.SerializerPool;
import org.exist.xmldb.XmldbURI;
import org.exist.xquery.CompiledXQuery;
import org.exist.xquery.Constants;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQuery;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.functions.request.RequestModule;
import org.exist.xquery.functions.response.ResponseModule;
import org.exist.xquery.functions.session.SessionModule;
import org.exist.xquery.value.Sequence;
import org.exist.debuggee.DebuggeeFactory;
import org.exist.dom.XMLUtil;

/**
 * Servlet to generate HTML output from an XQuery file.
 *
 * The servlet responds to an URL pattern as specified in the
 * WEB-INF/web.xml configuration file of the application. It will
 * interpret the path with which it is called as leading to a valid
 * XQuery file. The XQuery file is loaded, compiled and executed.
 * Any output of the script is sent back to the client.
 *
 * The servlet accepts the following initialization parameters in web.xml:
 *
 * <table border="0">
 * 	<tr><td>user</td><td>The user identity with which the script is executed.</td></tr>
 * 	<tr><td>password</td><td>Password for the user.</td></tr>
 * 	<tr><td>uri</td><td>A valid XML:DB URI leading to the root collection used to
 * 	process the request.</td></tr>
 * 	<tr><td>encoding</td><td>The character encoding used for XQuery files.</td></tr>
 * 	<tr><td>container-encoding</td><td>The character encoding used by the servlet
 * 	container.</td></tr>
 * 	<tr><td>form-encoding</td><td>The character encoding used by parameters posted
 * 	from HTML for
 * ms.</td></tr>
 * </table>
 *
 * User identity and password may also be specified through the HTTP session attributes
 * "user" and "password". These attributes will overwrite any other settings.
 *
 * @author Wolfgang Meier (wolfgang@exist-db.org)
 */
public class XQueryServlet extends AbstractExistHttpServlet {
    
    private static final long serialVersionUID = 5266794852401553015L;

    private static final Logger LOG = Logger.getLogger(XQueryServlet.class);

    // Request attributes
    public static final String ATTR_XQUERY_USER = "xquery.user";
    public static final String ATTR_XQUERY_PASSWORD = "xquery.password";
//    public static final String ATTR_XQUERY_SOURCE = "xquery.source";
    public static final String ATTR_XQUERY_URL = "xquery.url";
    public static final String ATTR_XQUERY_REPORT_ERRORS = "xquery.report-errors";
    public static final String ATTR_XQUERY_ATTRIBUTE = "xquery.attribute";
    public static final String ATTR_TIMEOUT = "xquery.timeout";
    public static final String ATTR_MAX_NODES = "xquery.max-nodes";
    public static final String ATTR_MODULE_LOAD_PATH = "xquery.module-load-path";

    public final static XmldbURI DEFAULT_URI = XmldbURI.EMBEDDED_SERVER_URI.append(XmldbURI.ROOT_COLLECTION_URI);
    public final static String DEFAULT_CONTENT_TYPE = "text/html";
    
    public final static String DRIVER = "org.exist.xmldb.DatabaseImpl";
    
    private XmldbURI collectionURI = null;
    
    private String encoding = null;
    private String contentType = null;

    @Override
    public Logger getLog() {
        return LOG;
    }

    /* (non-Javadoc)
     * @see javax.servlet.GenericServlet#init(javax.servlet.ServletConfig)
     */
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        final String confCollectionURI = config.getInitParameter("uri");
        if(confCollectionURI == null) {
            collectionURI = DEFAULT_URI;
            
        } else {
            try {
                collectionURI = XmldbURI.xmldbUriFor(confCollectionURI);
                
            } catch (final URISyntaxException e) {
                throw new ServletException("Invalid XmldbURI for parameter 'uri': "+e.getMessage(),e);
            }
        }
        
        encoding = config.getInitParameter("encoding");
        if(encoding == null) {
            encoding = DEFAULT_ENCODING;
        }
        getLog().info("encoding = " + encoding);

        contentType = config.getInitParameter("content-type");
        if(contentType == null) {
            contentType = DEFAULT_CONTENT_TYPE;
        }
    }
    
    /* (non-Javadoc)
     * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        process(request, response);
    }
    
    /* (non-Javadoc)
     * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
        HttpServletRequest request = null;
        
        //For POST request, If we are logging the requests we must wrap HttpServletRequest in HttpServletRequestWrapper
        //otherwise we cannot access the POST parameters from the content body of the request!!! - deliriumsky
        final Descriptor descriptor = Descriptor.getDescriptorSingleton();
        if(descriptor != null) {
            if(descriptor.allowRequestLogging()) {
                request = new HttpServletRequestWrapper(req, getFormEncoding());
            } else {
                request = req;
            }
            
        } else {
            request = req;
        }
        
        process(request, response);
    }
    
    //-------------------------------
    // doPut and doDelete added by Andrzej Taramina (andrzej@chaeron.com)
    // Date: Sept/05/2007
    //
    // These methods were added so that you can issue an HTTP PUT or DELETE request and have it serviced by an XQuery.
    // NOTE: The XQuery referenced in the target URL of the request will be executed and the PUT/DELETE request will be passed to it
    //
    //-------------------------------
    
     /* (non-Javadoc)
     * @see javax.servlet.http.HttpServlet#doPut(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
        HttpServletRequest request = null;
        
        //For POST request, If we are logging the requests we must wrap HttpServletRequest in HttpServletRequestWrapper
        //otherwise we cannot access the POST parameters from the content body of the request!!! - deliriumsky
        final Descriptor descriptor = Descriptor.getDescriptorSingleton();
        if(descriptor != null) {
            if(descriptor.allowRequestLogging()) {
                request = new HttpServletRequestWrapper(req, getFormEncoding());
            } else {
                request = req;
            }
            
        } else {
            request = req;
        }
        
        process(request, response);
    }
    
    
    /* (non-Javadoc)
     * @see javax.servlet.http.HttpServlet#doDelete(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        process(request, response);
    }
    
    
    /**
     * Processes incoming HTTP requests for XQuery
     */
    protected void process(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        final ServletOutputStream sout = response.getOutputStream();
        final PrintWriter output = new PrintWriter(new OutputStreamWriter(sout, getFormEncoding()));

//        response.setContentType(contentType + "; charset=" + formEncoding);
        response.addHeader( "pragma", "no-cache" );
        response.addHeader( "Cache-Control", "no-cache" );

        final Long timeout;
        final String timeoutOpt = (String) request.getAttribute(ATTR_TIMEOUT);
        if (timeoutOpt != null) {
            try {
                timeout = Long.parseLong(timeoutOpt);
            } catch (final NumberFormatException e) {
                getLog().error(e.getMessage(), e);
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                sendError(request, output, "Bad timeout option: " + timeoutOpt);
                return;
            }
        } else {
            timeout = null;
        }

        final Integer maxNodes;
        final String maxNodesOpt = (String) request.getAttribute(ATTR_MAX_NODES);
        if (maxNodesOpt != null) {
            try{
                maxNodes = Integer.parseInt(maxNodesOpt);
            } catch (final NumberFormatException e) {
                getLog().error(e.getMessage(), e);
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                sendError(request, output, "Bad max-nodes option: " + maxNodesOpt);
                return;
            }
        } else {
            maxNodes = null;
        }


        final String path = path(request);

        final String moduleLoadPath = modulePath(request);

        final HttpSession session = request.getSession( false );

        final Subject subject = subject(request, session);

        final Source source;
        try {
            source = source(request, path, moduleLoadPath, subject);
        } catch (EXistException e) {
            getLog().error(e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            sendError(request, output, "Source can't be loaded");
            return;

        } catch (PermissionDeniedException e) {
            getLog().error(e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            sendError(request, output, "Permission denied");
            return;
        }

        if (source == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            sendError(request, output, "Source not found");
            return;
        }

        boolean reportErrors = false;
        final String errorOpt = (String) request.getAttribute(ATTR_XQUERY_REPORT_ERRORS);
        if (errorOpt != null) {
            reportErrors = errorOpt.equalsIgnoreCase("YES");
        }
        
        final String requestAttr = (String) request.getAttribute(ATTR_XQUERY_ATTRIBUTE);

        try (DBBroker broker = getPool().get(subject)) {
            final XQuery xquery = broker.getXQueryService();
            CompiledXQuery query = xquery.getXQueryPool().borrowCompiledXQuery(broker, source);

            XQueryContext context;
            if (query==null) {
               context = xquery.newContext(AccessContext.REST);
               context.setModuleLoadPath(moduleLoadPath);
               try {
                query = xquery.compile(context, source);
                   
               } catch (final XPathException ex) {
                   getLog().error(ex.getMessage(), ex);
                   response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                   sendError(request, output, "Cannot compile xquery: "+ ex.getMessage());
                   return;

               } catch (final IOException ex) {
                   getLog().error(ex.getMessage(), ex);
                   response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                   sendError(request, output, "I/O exception while compiling xquery: "+ ex.getMessage());
                   return;
               }
               
            } else {
               context = query.getContext();
               context.setModuleLoadPath(moduleLoadPath);
            }

            final Properties outputProperties = new Properties();
            outputProperties.put("base-uri", collectionURI.toString());
            
            context.declareVariable(RequestModule.PREFIX + ":request", new HttpRequestWrapper(request, getFormEncoding(), getContainerEncoding()));
            context.declareVariable(ResponseModule.PREFIX + ":response", new HttpResponseWrapper(response));
            context.declareVariable(SessionModule.PREFIX + ":session", ( session != null ? new HttpSessionWrapper( session ) : null ) );

            if (timeout != null) {
                context.getWatchDog().setTimeout(timeout);
            }

            if (maxNodes != null) {
                context.getWatchDog().setMaxNodes(maxNodes);
            }

            DebuggeeFactory.checkForDebugRequest(request, context);

            Sequence resultSequence;
            try {
                resultSequence = xquery.execute(query, null, outputProperties);
                
            } finally {
                context.runCleanupTasks();
                xquery.getXQueryPool().returnCompiledXQuery(source, query);
            }

            final String mediaType = outputProperties.getProperty(OutputKeys.MEDIA_TYPE);
            if (mediaType != null) {
                if (!response.isCommitted()) {
                    if (MimeTable.getInstance().isTextContent(mediaType)) {
                        response.setContentType(mediaType + "; charset=" + getFormEncoding());
                        response.setCharacterEncoding(getFormEncoding());
                    } else
                        response.setContentType(mediaType);
                }
            } else {
                String contentType = this.contentType;
                try {
                    contentType = getServletContext().getMimeType(path);
                    if (contentType == null) {
                        contentType = this.contentType;
                    }

                } catch (final Throwable e) {
                    contentType = this.contentType;

                } finally {
                    if (MimeTable.getInstance().isTextContent(contentType)) {
                        contentType += "; charset=" + getFormEncoding();
                    }
                    response.setContentType(contentType);
                }
            }
            
            if (requestAttr != null && (XmldbURI.API_LOCAL.equals(collectionURI.getApiName())) ) {
                request.setAttribute(requestAttr, resultSequence);
                
            } else {
                final Serializer serializer = broker.getSerializer();
                serializer.reset();

                final SerializerPool serializerPool = SerializerPool.getInstance();

                final SAXSerializer sax = (SAXSerializer) serializerPool.borrowObject(SAXSerializer.class);
                try {
                    sax.setOutput(output, outputProperties);
                    serializer.setProperties(outputProperties);
                    serializer.setSAXHandlers(sax, sax);
                    serializer.toSAX(resultSequence, 1, resultSequence.getItemCount(), false, false);

                } finally {
                    serializerPool.returnObject(sax);
                }
            }
            
        } catch (final PermissionDeniedException e) {
            getLog().error(e.getMessage(), e);

            if (getDefaultUser().equals(subject)) {
                getAuthenticator().sendChallenge(request, response);
            } else {
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                sendError(request, output, e.getMessage());
            }
            return;
           
        } catch (final Throwable e){
            getLog().error(e.getMessage(),e);
            
            if (reportErrors) {
                writeError(output, e);
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                sendError(request, output, e.getMessage());
            }
            return;
        }

        output.flush();
        output.close();
    }

    private String path(HttpServletRequest request) {
        //first, adjust the path
        String path = request.getPathTranslated();
        if(path == null) {
            path = request.getRequestURI().substring(request.getContextPath().length());
            final int p = path.lastIndexOf(';');
            if(p != Constants.STRING_NOT_FOUND)
            {path = path.substring(0, p);}
            path = getServletContext().getRealPath(path);
        }

        //second, perform descriptor actions
        final Descriptor descriptor = Descriptor.getDescriptorSingleton();
        if(descriptor != null && !descriptor.requestsFiltered()) {
            //logs the request if specified in the descriptor
            descriptor.doLogRequestInReplayLog(request);

            //map's the path if a mapping is specified in the descriptor
            path = descriptor.mapPath(path);
        }

        return path;
    }

    private String modulePath(HttpServletRequest request) {
        final Object loadPath = request.getAttribute(ATTR_MODULE_LOAD_PATH);
        if (loadPath != null) {
            return getValue(loadPath);
        } else {

            String requestPath = request.getRequestURI();
            final int p = requestPath.lastIndexOf("/");
            if(p != Constants.STRING_NOT_FOUND) {
                requestPath = requestPath.substring(0, p);
            }

            return getServletContext().getRealPath(requestPath.substring(request.getContextPath().length()));
        }
    }

    private Subject subject(HttpServletRequest request, HttpSession session) throws IOException {
        Subject subject = getDefaultUser();

        // to determine the user, first check the request attribute "xquery.user", then
        // the current session attribute "user"
        final Object userAttrib = request.getAttribute(ATTR_XQUERY_USER);

        if(userAttrib != null || (session != null && request.isRequestedSessionIdValid())) {
            final Object passwdAttrib = request.getAttribute(ATTR_XQUERY_PASSWORD);
            String username;
            String password;
            if (userAttrib != null) {
                username = getValue(userAttrib);
                password = getValue(passwdAttrib);
            } else {
                username = getSessionAttribute(session, "user");
                password = getSessionAttribute(session, "password");
            }

            //TODO authentication should use super.authenticate(...) !!!
            try {
                if( username != null && password != null ) {
                    Subject newSubject = getPool().getSecurityManager().authenticate(username, password);
                    if (newSubject != null && newSubject.isAuthenticated()) {
                        subject = newSubject;
                    }
                }

            } catch (final AuthenticationException e) {
                getLog().error("User can not be authenticated ("+username+").", e);
            }
        }

        if (subject == getDefaultUser()) {
            Subject requestUser = HttpAccount.getUserFromServletRequest(request);
            if (requestUser != null) {
                subject = requestUser;
            } else {
                requestUser = getAuthenticator().authenticate(request);
                if (requestUser != null) {
                    subject = requestUser;
                }
            }
        }

        return subject;
    }

    private Source source(HttpServletRequest request, String path, String moduleLoadPath, Subject subject) throws EXistException, IOException, PermissionDeniedException {
        final Object url = request.getAttribute(ATTR_XQUERY_URL);
        if (url != null) {
            try (DBBroker broker = getPool().get(subject)) {
                return SourceFactory.getSource(broker, moduleLoadPath, url.toString(), true);
            }

        } else {
            final File f = new File(path);
            if(!f.canRead()) {
                //response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                //sendError(output, "Cannot read source file: ", path);
                return null;
            }
            return new FileSource(f, encoding, true);
        }
    }

    private String getSessionAttribute(HttpSession session, String attribute) {
        final Object obj = session.getAttribute(attribute);
        return getValue(obj);
    }

    private String getValue(Object obj) {
        if(obj == null)
            {return null;}
        
        if(obj instanceof Sequence)
            try {
                return ((Sequence)obj).getStringValue();
            } catch (final XPathException e) {
                return null;
            }
        return obj.toString();
    }

    private void writeError(PrintWriter out, Throwable e) {
        out.print("<error>");
//        Throwable t = e.getCause();
//        if (t != null)
//            out.print(XMLUtil.encodeAttrMarkup(t.getMessage()));
//        else
        if (e.getMessage() != null) {
            out.print(XMLUtil.encodeAttrMarkup(e.getMessage()));
        }
        out.println("</error>");

        out.flush();
        out.close();
    }

    private void sendError(HttpServletRequest request, PrintWriter out, String message) {
        out.print("<html><head>");
        out.print("<title>XQueryServlet Error</title>");
        out.print("<link rel=\"stylesheet\" type=\"text/css\" href=\"error.css\"></link></head>");
        out.println("<body><h1>Error found</h1>");
        out.print("<div class='request'><b>Request: </b>");
        out.print(XMLUtil.encodeAttrMarkup(request.getRequestURI()));
        out.println("</div>");
        out.print("<div class='message'><b>Message: </b>");
        out.print(XMLUtil.encodeAttrMarkup(message));
        out.print("</div>");
//        out.print("<div class='description'><pre>");
//        out.print(XMLUtil.encodeAttrMarkup(description));
//        out.print("</pre></div>");
        out.print("</body></html>");

        out.flush();
        out.close();
    }
}
