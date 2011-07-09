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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import nl.ou.netlogo.sql.wrapper.SqlConnection.ConnectionEvent;

import org.nlogo.api.Agent;
import org.nlogo.api.Context;
import org.nlogo.api.ExtensionException;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.ConnectionHandle;

/**
 * Class used to implement the storage of multiple database connections for a
 * NetLogo model.
 * 
 * @author NetLogo project-team
 * 
 */
public class SqlConnectionManager implements SqlConfigurable, EventObserver<ConnectionEvent> {

    private static final Logger LOG = SqlLogger.getLogger();
    /**
     * For any connection pool, a hard minimum count of connections should be
     * available. The wrapper will throw an exception when it is configured with
     * a count that is effectively lower.
     */
    private static final int MIN_CONNECTIONS = 5;
    private int partitions;
    private int maxConnections;
    private long connectionPoolTimeout;

    /**
     * Connection pool used for the connections in the model context.
     */
    private Map<Agent, SqlConnection> connections = new ConcurrentHashMap<Agent, SqlConnection>();

    private DatabaseInfo dbInfo = null;

    /**
     * the pool of physical connections towards a database when the default
     * connection properties are used.
     */
    private BoneCP connectionPool;

    public SqlConnectionManager() {
        LOG.fine("SqlConnectionManager constructor");
    }

    /**
     * Method used to check if the connection manager is using a connection pool
     * to manage physical database connections
     * 
     * @return TRUE if so, FALSE otherwise.
     */
    public boolean connectionPoolEnabled() {
        if (this.connectionPool != null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Method used to initialize the default connection pool when the command
     * sql:set-default "connect" [params]" is executed. As we do not know at
     * which time it is executed and how many times, any existing pool is
     * dropped. The JDBC driver is loaded to be on the safe side.
     */
    public void initDefaultConnectionPool() {
        LOG.fine("SqlConnectionManager.initDefaultConnectionPool()");
        try {
            /*
             * If we already had an active pool, shutdown and create a new one.
             */
            shutdownConnectionPool();

            /*
             * Make sure the correct driver is loaded before we continue.
             */
            Class.forName(dbInfo.getDriverClass());

            if (partitions < 1) {
                String message = "partitions is less than 1, configure at minimum of 1";
                LOG.severe(message);
                throw new ExtensionException(message);
            }

            int connectionsPerPartition = maxConnections / partitions;
            if (connectionsPerPartition * partitions < MIN_CONNECTIONS) {
                String message = "Effective amount of connections is less than " + MIN_CONNECTIONS
                        + ". Configure max-connections and partitions so that there are at least " + MIN_CONNECTIONS
                        + " connections in total.";
                LOG.severe(message);
                throw new ExtensionException(message);
            }
            LOG.info("Connections per partition: " + connectionsPerPartition);
            BoneCPConfig config = new BoneCPConfig();
            config.setUsername(dbInfo.getUser());
            config.setPassword(dbInfo.getPassword());
            config.setPartitionCount(partitions);
            config.setMaxConnectionsPerPartition(connectionsPerPartition);
            if (config.getMinConnectionsPerPartition() < connectionsPerPartition) {
                config.setMinConnectionsPerPartition(connectionsPerPartition);
            }
            config.setJdbcUrl(dbInfo.getJdbcUrl());
            config.setConnectionHook(new ConnectionHook());

            LOG.finest(String.format("Trying to access database '%s', user '%s', password '%s'", dbInfo.getJdbcUrl(),
                    dbInfo.getUser(), dbInfo.getPassword()));
            connectionPool = new BoneCP(config);
        } catch (Exception e) {
            LOG.severe("SqlConnectionManager.initDefaultConnectionPool() failed with Exception");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Shuts down the existing connection pool, releasing connections etc in the
     * process.
     */
    protected void shutdownConnectionPool() {
        // If we already had an active pool, shutdown
        // and create a new one.
        if (connectionPool != null) {
            LOG.info("Shutting down connection pool");
            // Close existing connections provided by the connection pool
            for (SqlConnection sqlConnection : connections.values()) {
                if (sqlConnection.getConnection() instanceof ConnectionHandle) {
                    LOG.fine("Removing pooled connection " + sqlConnection);
                    sqlConnection.close();
                }
            }
            connectionPool.shutdown();
            connectionPool = null;
        }
    }

    /**
     * Closes all open connections associated with agents.
     */
    protected void closeAll() {
        LOG.info("Closing all connections");
        for (SqlConnection sqlConnection : connections.values()) {
            LOG.fine("Closing connection " + sqlConnection);
            sqlConnection.close();
        }
    }

    /**
     * Register an SqlConnection to an agent
     * 
     * @param agent
     *            Agent Object
     * @param connection
     *            SqlConnection object
     */
    private void registerConnection(Agent agent, SqlConnection connection) {
        LOG.fine("SqlConnectionManager.registerConnection(this: " + this + ", agent: " + agent + ")");
        connection.register(this);
        connections.put(agent, connection);
    }

    /**
     * Method used to retrieve a connection handle based on a passed in key
     * (agent object).
     * 
     * @param agent
     *            Agent object
     * @param createConnection
     *            <code>true</code> create a new connection if the agent has no
     *            open connection and a connection pool exists
     * @return connection handle, null when no connection was found or no
     *         connection pool was available to create a new connection
     * @throws ExtensionException
     *             For failure to create a new connection
     */
    public SqlConnection getConnection(Agent agent, boolean createConnection) throws ExtensionException {
        LOG.fine("SqlConnectionManager.getConnection(agent: " + agent + ", createConnection:" + createConnection + ")");
        SqlConnection sqlconn = null;

        sqlconn = connections.get(agent);
        if (createConnection && connectionPool != null && (sqlconn == null || !sqlconn.isConnected())) {
            // fetch a connection from the pool
            try {
                LOG.finest("SqlConnectionManager.getConnection(agent: " + agent + ", createConnection:"
                        + createConnection + "): fetching a connection from the pool");
                sqlconn = createConnectionFromPool(agent);
            } catch (SQLException e) {
                throw new ExtensionException(e);
            }
        } else {
            LOG.finest("SqlConnectionManager.getConnection(agent: " + agent + ", createConnection:" + createConnection
                    + "): hitting the empty else branch. connectionPool: " + connectionPool);
        }

        LOG.fine("SqlConnectionManager.getConnection(): returning " + sqlconn);
        return sqlconn;

    }

    /**
     * Creates a new SqlConnection from the connection pool and registers it to
     * the supplied agent.
     * 
     * @param agent
     *            Agent object which will use this connection
     * @return SqlConnection
     * @throws SQLException
     */
    private SqlConnection createConnectionFromPool(Agent agent) throws SQLException {
        Connection conn = null;

        /**
         * Implements logic to get a connection in a thread so that a timeout
         * can be imposed
         * 
         * @author NetLogo project-team
         * 
         */
        class ConnectionGetter extends Thread {
            Semaphore binSem;
            Connection conn = null;

            ConnectionGetter(Semaphore binSem) {
                this.binSem = binSem;
            }

            public void run() {
                /*
                 * First obtain the lock, and notify the main thread it can
                 * continue
                 */
                try {
                    LOG.finest("ConnectionGetter.run(): before connectionPool.getConnection()");
                    this.conn = connectionPool.getConnection();
                    LOG.finest("ConnectionGetter.run(): after connectionPool.getConnection()");
                    LOG.finest("ConnectionGetter.run(): before this.binSem.release()");
                    this.binSem.release();
                    LOG.finest("ConnectionGetter.run(): after this.binSem.release()");
                } catch (SQLException sqlex) {
                    LOG.log(Level.SEVERE, "connectionPool.createConnectionFromPool() failed : ", sqlex);
                    this.conn = null;
                }
            }

            public Connection getConnection() {
                return this.conn;
            }
        }

        /*
         * Getting a connection from the pool can block forever if the pool is
         * exhausted and no connections are closed. Try to obtain the connection
         * in a thread, and impose a timeout
         */
        Semaphore binSem = new Semaphore(1);
        try {
            binSem.acquire();
        } catch (InterruptedException intex) {
            String message = "connectionPool.createConnectionFromPool() interrupted: " + intex;
            LOG.severe(message);
            Thread.currentThread().interrupt();
            throw new SQLException(message);
        }
        ConnectionGetter getterThread = new ConnectionGetter(binSem);
        getterThread.start();
        try {
            /*
             * try to acquire the semaphore with a timeout. If the configured
             * timeout is 0, don't try to acquire the semaphore (and be willing
             * to block indefinitely)
             */
            if ((this.connectionPoolTimeout > 0 && binSem.tryAcquire(this.connectionPoolTimeout /*
                                                                                                 * +
                                                                                                 * 2
                                                                                                 */, TimeUnit.SECONDS))
                    || this.connectionPoolTimeout == 0) {
                // success, get the connection from the thread
                if (this.connectionPoolTimeout == 0) {
                    // will block here if BoneCP (connection pool) blocks
                    binSem.acquireUninterruptibly();
                }
                conn = getterThread.getConnection();
            } else {
                /*
                 * obtaining the connection did not succeed within the timeout
                 * period, terminate the thread and report failure
                 */
                getterThread.interrupt();
                String message = "connectionPool.createConnectionFromPool() timed out";
                LOG.severe(message);
                throw new SQLException(message);
            }
        } catch (InterruptedException intex) {
            String message = "connectionPool.createConnectionFromPool() was interrupted: " + intex;
            LOG.severe(message);
            Thread.currentThread().interrupt();
            throw new SQLException(message);
        }
        if (conn != null) {
            SqlConnection sqlConn = new SqlConnection(conn, dbInfo);
            registerConnection(agent, sqlConn);
            return sqlConn;
        } else {
            throw new SQLException("Unable to obtain a database connection");
        }
    }

    /**
     * Creates a new unmanaged connection for the specified agent.
     * 
     * @param agent
     *            Agent object
     * @param connectionSettings
     *            Connection settings
     * @return SqlConnection
     * @throws ExtensionException
     */
    public SqlConnection createConnection(Agent agent, SqlSetting connectionSettings) throws ExtensionException {
        try {
            DatabaseInfo localDbInfo = DatabaseFactory.createDatabaseInfo(connectionSettings);
            SqlConnection sqlConn = createConnection(localDbInfo);
            registerConnection(agent, sqlConn);
            return sqlConn;
        } catch (DatabaseConfigurationException ex) {
            throw new ExtensionException(ex);
        }
    }

    /**
     * Creates a new unmanaged connection
     * 
     * @param myDbInfo
     *            DatabaseInfo for a specific database engine
     * @return SqlConnection
     * @throws ExtensionException
     */
    private SqlConnection createConnection(DatabaseInfo myDbInfo) throws ExtensionException {
        try {
            Class.forName(myDbInfo.getDriverClass());
            Connection conn = DriverManager.getConnection(myDbInfo.getJdbcUrl(), myDbInfo.getUser(),
                    myDbInfo.getPassword());

            SqlConnection sqlConn = new SqlConnection(conn, myDbInfo);
            return sqlConn;
        } catch (ClassNotFoundException e) {
            throw new ExtensionException("Unable to load database driver");
        } catch (SQLException e) {
            throw new ExtensionException(e);
        }
    }

    @Override
    public void configure(SqlSetting settings, Context context) throws Exception {
        if (settings.getName().equals(SqlConfiguration.DEFAULTCONNECTION)) {
            configureDatabase(settings);
        } else if (settings.getName().equals(SqlConfiguration.CONNECTIONPOOL)) {
            configureConnectionPool(settings);
        }
    }

    private void configureConnectionPool(SqlSetting settings) throws ExtensionException {
        if (!settings.isValid()) {
            return;
        }
        try {
            partitions = settings.getInt(SqlConfiguration.CONNECTIONPOOL_OPT_PARTITIONS);
            maxConnections = settings.getInt(SqlConfiguration.CONNECTIONPOOL_OPT_MAXCONNECTIONS);
            connectionPoolTimeout = settings.getLong(SqlConfiguration.CONNECTIONPOOL_OPT_TIMEOUT);
            LOG.fine("Configured connection pool:");
            LOG.fine("    Partitions: " + partitions);
            LOG.fine("    Connections (max): " + maxConnections);
            LOG.fine("    Timeout: " + connectionPoolTimeout);
        } catch (Exception e) {
            throw new ExtensionException("Exception while configuring connection pool: " + e);
        }
    }

    /**
     * Configures the default database connection based on the SqlSetting object
     * provided.
     * 
     * @param settings
     *            SqlSetting object
     * @throws DatabaseConfigurationException
     *             For configuration errors
     */
    private void configureDatabase(SqlSetting settings) throws DatabaseConfigurationException {
        if (!settings.isValid()) {
            return;
        }
        dbInfo = DatabaseFactory.createDatabaseInfo(settings);
        initDefaultConnectionPool();
    }

    /**
     * Removes the SqlConnection object from the connections map.
     * 
     * @param sqlConnection
     *            SqlConnection object to remove
     */
    private void remove(SqlConnection sqlConnection) {
        LOG.fine("Removing SqlConnection " + sqlConnection + " from connections-map");
        connections.values().remove(sqlConnection);
    }

    @Override
    public void notify(ConnectionEvent event, EventObservable<ConnectionEvent> observable) {
        LOG.fine("Notified of event " + event + " on " + observable);
        switch (event) {
        case CLOSE:
            remove((SqlConnection) observable);
            break;
        case AUTO_DISCONNECT:
            // Do nothing SqlConnection needs to be retained
            break;
        }
    }
}
