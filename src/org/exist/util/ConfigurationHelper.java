package org.exist.util;

import java.io.File;
import java.net.URL;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.storage.BrokerPool;
import org.exist.xmldb.DatabaseImpl;

public class ConfigurationHelper {
    private final static Logger LOG = LogManager.getLogger(ConfigurationHelper.class); //Logger

  public static Optional<Path> existHome() {
    return existHome(DatabaseImpl.CONF_XML);
  }

  public static Optional<Path> existHome(String config) {
    File path = getExistHome(config);
    if (path == null) {
      return Optional.empty();
    }

    return Optional.of(path.toPath());
  }

    /**
     * Returns a file handle for eXist's home directory.
     * Order of tests is designed with the idea, the more precise it is,
     * the more the developer know what he is doing
     * <ol>
     *   <li>Brokerpool      : if eXist was already configured.
     *   <li>exist.home      : if exists
     *   <li>user.home       : if exists, with a conf.xml file
     *   <li>user.dir        : if exists, with a conf.xml file
     *   <li>classpath entry : if exists, with a conf.xml file
     * </ol>
     *
     * @return the file handle or <code>null</code>
     */
    public static File getExistHome() {
    	return getExistHome(DatabaseImpl.CONF_XML);
    }

    /**
     * Returns a file handle for eXist's home directory.
     * Order of tests is designed with the idea, the more precise it is,
     * the more the developper know what he is doing
     * <ol>
     *   <li>Brokerpool      : if eXist was already configured.
     *   <li>exist.home      : if exists
     *   <li>user.home       : if exists, with a conf.xml file
     *   <li>user.dir        : if exists, with a conf.xml file
     *   <li>classpath entry : if exists, with a conf.xml file
     * </ol>
     *
     * @return the file handle or <code>null</code>
     */
    public static File getExistHome(String config) {
    	File existHome = null;
    	
    	// If eXist was already configured, then return 
    	// the existHome of this instance.
    	try {
    		final BrokerPool broker = BrokerPool.getInstance();
    		if(broker != null) {
    			existHome = broker.getConfiguration().getExistHome();
			if(existHome!=null) {
                        	LOG.debug("Got eXist home from broker: " + existHome);
    				return existHome;
			}
    		}
    	} catch(final Throwable e) {
            // Catch all potential problems
            LOG.debug("Could not retrieve instance of brokerpool: " + e.getMessage());
    	}
    	
        // try exist.home
        if (System.getProperty("exist.home") != null) {
            existHome = ConfigurationHelper.decodeUserHome(System.getProperty("exist.home")).toFile();
            if (existHome.isDirectory()) {
                LOG.debug("Got eXist home from system property 'exist.home': " + existHome);
                return existHome;
            }
        }
        
        // try user.home
        existHome = new File(System.getProperty("user.home"));
        if (existHome.isDirectory() && new File(existHome, config).isFile()) {
            LOG.debug("Got eXist home from system property 'user.home': " + existHome);
            return existHome;
        }
        
        
        // try user.dir
        existHome = new File(System.getProperty("user.dir"));
        if (existHome.isDirectory() && new File(existHome, config).isFile()) {
            LOG.debug("Got eXist home from system property 'user.dir': " + existHome);
            return existHome;
        }
        
        // try classpath
        final URL configUrl = ConfigurationHelper.class.getClassLoader().getResource(config);
        if (configUrl != null) {
            existHome = new File(configUrl.getPath()).getParentFile();
            LOG.debug("Got eXist home from classpath: " + existHome);
            return existHome;
        }
        
        existHome = null;
        return existHome;
    }

  /**
   * Returns a file handle for the given path, while <code>path</code> specifies
   * the path to an eXist configuration file or directory.
   * <br>
   * Note that relative paths are being interpreted relative to <code>exist.home</code>
   * or the current working directory, in case <code>exist.home</code> was not set.
   *
   * @param path the file path
   * @return the file handle
   */
  public static Path lookupPath(final String path) {
    return lookup(path, Optional.empty());
  }

  public static File lookup(final String path) {
    return lookup(path, Optional.empty()).toFile();
  }

  /**
   * Returns a file handle for the given path, while <code>path</code> specifies
   * the path to an eXist configuration file or directory.
   * <br>
   * If <code>parent</code> is null, then relative paths are being interpreted
   * relative to <code>exist.home</code> or the current working directory, in
   * case <code>exist.home</code> was not set.
   *
   * @param path path to the file or directory
   * @param parent parent directory used to lookup <code>path</code>
   * @return the file handle
   */
  public static Path lookup(final String path, final Optional<Path> parent) {
    // resolvePath is used for things like ~user/folder
    final Path p = decodeUserHome(path);
    if (p.isAbsolute()) {
      return p;
    }

    return parent
        .orElse(existHome().orElse(Paths.get(System.getProperty("user.dir"))))
        .resolve(path);
  }


  /**
   * Resolves the given path by means of eventually replacing <tt>~</tt> with the users
   * home directory, taken from the system property <code>user.home</code>.
   *
   * @param path the path to resolve
   * @return the resolved path
   */
  public static Path decodeUserHome(final String path) {
    if (path != null && path.startsWith("~") && path.length() > 1) {
      return Paths.get(System.getProperty("user.home")).resolve(path.substring(1));
    } else {
      return Paths.get(path);
    }
  }
}
