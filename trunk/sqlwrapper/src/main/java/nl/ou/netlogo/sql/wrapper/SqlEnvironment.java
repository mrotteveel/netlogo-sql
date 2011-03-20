/*
 * Copyright 2010, 2011 Open University of The Netherlands
 * Contributors: Jan Blom, Rene Quakkelaar, Mark Rotteveel
 *
 * This file is part of NetLogo SQL Wrapper extension.
 * 
 * NetLogo SQL Wrapper extension is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public License 
 * as published by the Free Software Foundation, either version 3 of the 
 * License, or (at your option) any later version.
 * 
 * NetLogo SQL Wrapper is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with NetLogo SQL Wrapper extension.  If not, 
 * see <http://www.gnu.org/licenses/>.
 */
package nl.ou.netlogo.sql.wrapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.nlogo.api.Agent;
import org.nlogo.api.Context;
import org.nlogo.api.ExtensionException;

/**
 * Class representing the SQLEnvironment object for the NetLogo SQL Extension.
 * Each connection in a model towards a relational will result in an instance of
 * a SQLEnvironment, which is associated with an individual agent.
 * 
 * @author NetLogo project-team
 * 
 */
public class SqlEnvironment {

    private SqlConnectionManager connectionManager = new SqlConnectionManager();
    private SqlConfiguration configuration = new SqlConfiguration();
    private Logger LOG = SqlLogger.getLogger();

    private static String mavenVersion;

    private static final String VERSION_TEMPLATE = "Sql Extension Version %s";

    /**
     * List of getter and setter functions for this class
     */
    public SqlConnectionManager getConnectionManager() {
        LOG.log(Level.FINE, "SqlEnvironment.getConnectionManager()");
        return connectionManager;
    }

    /**
     * Method used to retrieve the connection for a context.
     * 
     * @param context
     * @param createConnection
     *            <code>true</code> create a new connection if the agent has no
     *            open connection and a connection pool exists
     * @return connection handle, null when no connection was found or no
     *         connection pool was available to create a new connection
     * @throws ExtensionException
     */
    public SqlConnection getSqlConnection(Context context, boolean createConnection) throws ExtensionException {
        LOG.log(Level.FINE, "SqlEnvironment.getSqlConnection(context=" + context + ")");
        Agent agent = context.getAgent();

        if (agent == null) {
            String problem = "Cannot determine agent.";
            LOG.log(Level.SEVERE, "SqlEnvironment.getSqlConnection(context=" + context + "): " + problem);
            throw new ExtensionException(problem);
        }

        return connectionManager.getConnection(agent, createConnection);
    }

    /**
     * Method used to retrieve the connection for a context, will return an
     * exception if no active connection was available.
     * 
     * @param context
     * @param createConnection
     *            <code>true</code> create a new connection if the agent has no
     *            open connection and a connection pool exists
     * @return active connection
     * @throws ExtensionException
     */
    public SqlConnection getActiveSqlConnection(Context context, boolean createConnection) throws ExtensionException {
        LOG.log(Level.FINE, "SqlEnvironment.getSqlActiveConnection(context=" + context + ")");
        SqlConnection sqlc = getSqlConnection(context, createConnection);
        if (sqlc == null) {
            String problem = "No active database connection available";
            LOG.log(Level.SEVERE, "SqlEnvironment.getSqlActiveConnection(context=" + context + "): " + problem
                    + "(sqlc == null)");
            throw new ExtensionException(problem);
        }
        LOG.log(Level.FINE, "SqlEnvironment.getSqlActiveConnection(context=" + context + ")" + "returns SqlConnection("
                + sqlc + ")");
        return sqlc;
    }

    /**
     * Creates a new unmanaged connection for a context.
     * 
     * @param context
     *            Context for the connection
     * @param host
     *            Hostname
     * @param port
     *            Port
     * @param user
     *            User
     * @param passwd
     *            Password
     * @param database
     *            Database schema
     * @return SqlConnection
     * @throws ExtensionException
     */
    public SqlConnection createConnection(Context context, String host, int port, String user, String passwd,
            String database) throws ExtensionException {
        Agent agent = context.getAgent();

        if (agent == null) {
            throw new ExtensionException("Cannot determine agent.");
        }

        return connectionManager.createConnection(agent, host, port, user, passwd, database);
    }

    /**
     * Method used to return the version string of the SQL Extension.
     * 
     * @return version string
     */
    public String getVersion() {
        if (mavenVersion == null) {
            InputStream is = null;
            try {
                is = getClass().getResourceAsStream("/META-INF/maven/nl.ou.netlogo/sql/pom.properties");
                Properties pomProperties = new Properties();
                if (is == null) {
                    LOG.warning("Resource /META-INF/maven/nl.ou.netlogo/sql/pom.properties not found; not running as maven packaged artifact?");
                } else {
                    pomProperties.load(is);
                }
                mavenVersion = pomProperties.getProperty("version", "Unknown");
            } catch (IOException ex) {
                LOG.warning("Unable to load /META-INF/maven/nl.ou.netlogo/sql/pom.properties; not running as maven packaged artifact?");
                mavenVersion = "Unknown";
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException ex) {
                        // ignore
                    }
                }
            }
        }

        return String.format(VERSION_TEMPLATE, mavenVersion);
    }

    /**
     * 
     * @return the SQL configuration
     */
    public SqlConfiguration getConfiguration() {
        return configuration;
    }
}
