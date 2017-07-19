/*
 * ServerShutdown.java - Jul 20, 2003
 * 
 * @author wolf
 */
package org.exist.jetty;

import org.apache.xmlrpc.XmlRpcException;
import org.exist.util.ConfigurationHelper;
import org.exist.util.SystemExitCodes;
import org.exist.xmldb.DatabaseInstanceManager;
import org.exist.xmldb.XmldbURI;

import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.XMLDBException;
import se.softhouse.jargo.Argument;
import se.softhouse.jargo.ArgumentException;
import se.softhouse.jargo.CommandLineParser;
import se.softhouse.jargo.ParsedArguments;

import java.io.IOException;
import java.io.InputStream;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.exist.util.ArgumentUtil.getOpt;
import static se.softhouse.jargo.Arguments.helpArgument;
import static se.softhouse.jargo.Arguments.stringArgument;

/**
 * Call the main method of this class to shut down a running database instance.
 *
 * @author wolf
 */
public class ServerShutdown {

    /* general arguments */
    private static final Argument<?> helpArg = helpArgument("-h", "--help");

    /* database connection arguments */
    private static final Argument<String> userArg = stringArgument("-u", "--user")
        .description("specify username (has to be a member of group dba).")
        .defaultValue("admin")
        .build();
    private static final Argument<String> passwordArg = stringArgument("-p", "--password")
        .description("specify password for the user.")
        .defaultValue("")
        .build();
    private static final Argument<String> uriArg = stringArgument("-l", "--uri")
        .description("the XML:DB URI of the database instance to be shut down.")
        .build();

    @SuppressWarnings("unchecked")
    public static void main(final String[] args) {
        try {
            final ParsedArguments arguments = CommandLineParser
                .withArguments(userArg, passwordArg, uriArg)
                .andArguments(helpArg)
                .parse(args);

            process(arguments);
        } catch (final ArgumentException e) {
            System.out.println(e.getMessageAndUsage());
            System.exit(SystemExitCodes.INVALID_ARGUMENT_EXIT_CODE);

        }
    }

    private static void process(final ParsedArguments arguments) {
        final Properties properties = loadProperties();

        final String user = arguments.get(userArg);
        final String passwd = arguments.get(passwordArg);

        String uri = getOpt(arguments, uriArg)
            .orElseGet(() -> properties.getProperty("uri", "xmldb:exist://localhost:8080/exist/xmlrpc"));

        try {
            // initialize database drivers
            final Class<?> cl = Class.forName("org.exist.xmldb.DatabaseImpl");
            // create the default database
            final Database database = (Database) cl.newInstance();
            DatabaseManager.registerDatabase(database);
            if (!uri.endsWith(XmldbURI.ROOT_COLLECTION)) {
                uri = uri + XmldbURI.ROOT_COLLECTION;
            }
            final Collection root = DatabaseManager.getCollection(uri, user, passwd);
            final DatabaseInstanceManager manager = (DatabaseInstanceManager) root
                .getService("DatabaseInstanceManager", "1.0");
            System.out.println("Shutting down database instance at ");
            System.out.println('\t' + uri);
            manager.shutdown();

        } catch (final XMLDBException e) {
            System.err.println("ERROR: " + e.getMessage());

            final Throwable t = e.getCause();
            if(t!=null && t instanceof XmlRpcException){
                System.err.println("CAUSE: "+t.getMessage());
            } else {
                e.printStackTrace();
            }

        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties loadProperties() {
        final Path propFile = ConfigurationHelper.lookup("client.properties").toPath();
        final Properties properties = new Properties();
        try {
            if (Files.isReadable(propFile)) {
                try(final InputStream pin = Files.newInputStream(propFile)) {
                    properties.load(pin);
                }
            } else {
                try(final InputStream pin = ServerShutdown.class.getResourceAsStream("client.properties")) {
                    properties.load(pin);
                }
            }
        } catch (final IOException e) {
            System.err.println("WARN - Unable to load properties from: " + propFile.toAbsolutePath().toString());
        }
        return properties;
    }
}