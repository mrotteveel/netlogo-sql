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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.nlogo.api.LogoList;
import org.nlogo.api.ExtensionException;

import com.jolbox.bonecp.ConnectionHandle;

/**
 * Connection object for the plugin.
 * 
 * @author NetLogo 2010 team
 */
public class SqlConnection implements
		EventObservable<SqlConnection.ConnectionEvent> {

	private static final Logger LOG = SqlLogger.getLogger();

	private DatabaseInfo dbInfo;
	private Connection connection;
	private SqlStatement statement;
	private List<EventObserver<ConnectionEvent>> connectionObservers = Collections
			.synchronizedList(new ArrayList<EventObserver<ConnectionEvent>>());
	protected final AutodisconnectCoordinator autodisconnectCoordinator = new AutodisconnectCoordinator();

	/**
	 * Constructor used when a SqlConnection is implicitly created via the
	 * default connection url and the third party connection pool.
	 * 
	 * @param conn
	 */
	/*protected SqlConnection(Connection conn) {
		this.connection = conn;
	}*/

	protected SqlConnection(Connection conn, DatabaseInfo dbInfo) {
		this.connection = conn;
		this.dbInfo = dbInfo;
	}

	/**
	 * Method used to fully disconnect from the database. Fires the ConnectionEvent.CLOSE.
	 */
	public void close() {
		LOG.log(Level.FINE, "SqlConnection.close()");
		try {
			closeStatement();
			closePhysicalConnection();
		} finally {
			for (EventObserver<ConnectionEvent> observer : connectionObservers) {
				observer.notify(ConnectionEvent.CLOSE, this);
			}
		}
	}
	
	/**
	 * Method for auto-disconnect, closes the physical connection only. Fires the ConnectionEvent.AUTO_DISCONNECT.
	 */
	protected void autoDisconnect() {
		LOG.fine("Performing autoDisconnect");
		
		try {
			closePhysicalConnection();
		} finally {
			for (EventObserver<ConnectionEvent> observer : connectionObservers) {
				observer.notify(ConnectionEvent.AUTO_DISCONNECT, this);
			}
		}
	}
	
	/**
	 * Closes the physical connection to the database.
	 */
	protected void closePhysicalConnection() {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				LOG.log(Level.FINE, "Closing connection failed (ignored)", e);
			} finally {
				connection = null;
			}
		}
	}
	
	/**
	 * Returns the connection handle, which could be null.
	 * 
	 * @return Connection object
	 */
	protected Connection getConnection() {
		LOG.log(Level.FINE, "SqlConnection.getConnection()");
		return connection;
	}

	/**
	 * Method used to indicate we have an active connection. We assume the
	 * connection itself is healthy when initialized.
	 * 
	 * @return true or false
	 */
	public boolean isConnected() {
		LOG.log(Level.FINE, "SqlConnection.isConnected()");
		if (connection != null) {
			try {
				return !connection.isClosed();
			} catch (SQLException e) {
				return false;
			}
		} else {
			return false;
		}
	}

	/**
	 * Closes associated statement and its resultset.
	 */
	public void closeStatement() {
		if (this.statement != null) {
			this.statement.close();
			this.statement = null;
		}
	}

	/**
	 * 
	 * @return Resultset associated with this connection (or null of no
	 *         resultset is open).
	 */
	public SqlResultSet getResultSet() {
		if (statement != null) {
			return statement.getResultSet();
		} else {
			return null;
		}
	}

	/**
	 * 
	 * @return Number of rows affected by an update statement (INSERT, UPDATE,
	 *         DELETE), or -1 when no statement has been executed yet.
	 */
	public Double getRowCount() {
		if (statement != null) {
			return Double.valueOf(statement.getRowCount());
		} else {
			return Double.valueOf(-1);
		}
	}

	/**
	 * Creates a SqlStatement with the specified sql statement. The SqlStatement
	 * is also associated with this SqlConnection.
	 * 
	 * @param sql
	 *            SQL statement (no parameters)
	 * @return SqlStatement object
	 * @throws SQLException
	 */
	public SqlStatement createStatement(String sql) throws SQLException {
		LOG.fine("SqlConnection.createStatement('" + sql + "'");
		return createStatement(sql, null);
	}

	/**
	 * Method used to return the current database context for the active connection.
	 * @return databasename
	 * @throws DatabaseFeatureNotImplementedException
	 */
	public String currentDatabase() throws DatabaseFeatureNotImplementedException {
		return dbInfo.getCurrentDatabase(this);
	}
	
	/**
	 * Method used to switch database context for the active connection
	 * @param schemaName
	 * @throws DatabaseFeatureNotImplementedException
	 * @throws ExtensionException
	 */
	public void useDatabase(String schemaName) throws DatabaseFeatureNotImplementedException, ExtensionException {
		if (connection instanceof ConnectionHandle) {
			throw new ExtensionException("sql:use-database is only allowed on connections created using sql:connect; this is a connection from the connection pool");
		}
		dbInfo.useDatabase(this, schemaName);
	}
	
	/**
	 * Method used to check if a certain database exists.
	 * 
	 * @param schemaName
	 * @return TRUE if exists, FALSE otherwise.
	 * @throws DatabaseFeatureNotImplementedException
	 */
	public boolean findDatabase(String schemaName) throws DatabaseFeatureNotImplementedException{
		return dbInfo.findDatabase(this, schemaName);
	}
	
	/**
	 * Method used to switch on auto-commit for the active database connection.
	 * 
	 * @throws ExtensionException
	 */
	public void autoCommitOn() throws ExtensionException {
		try {
			connection.setAutoCommit(true);
			autodisconnectCoordinator.commit();
		} catch (Exception ex) {
			throw new ExtensionException("Could not enable autocommit");
		}
	}
	
	/**
	 * Method used to switch off autocommit for the active connection.
	 * 
	 * @throws ExtensionException
	 */
	public void autoCommitOff() throws ExtensionException {
		try {
			connection.setAutoCommit(false);
		} catch (Exception ex) {
			throw new ExtensionException("Could not disable autocommit");
		}
	}
	
	/**
	 * Method used to check the status of autocommit for the active connection.
	 * 
	 * @return TRUE if active, FALSE otherwise
	 * @throws ExtensionException
	 */
	public boolean autoCommitEnabled() {
		try {
			return connection.getAutoCommit();
		}
		catch ( Exception e ) {
			return false;
		}
	}
	
	/**
	 * Method used to start a SQL transaction.
	 * 
	 * @throws ExtensionException
	 */
	public void startTransaction() throws ExtensionException{
		autoCommitOff();
	}

	/**
	 * Method used to commit a SQL transaction.
	 * 
	 * @throws ExtensionException
	 */
	public void commitTransaction() throws ExtensionException{
		try {
			connection.commit();
			autodisconnectCoordinator.commit();
		}
		catch ( Exception e ) {
			throw new ExtensionException("Could not commit the current transaction.");
		}
	}

	/**
	 * Method used to rollback a SQL transaction.
	 * 
	 * @throws ExtensionException
	 */
	public void rollbackTransaction() throws ExtensionException{
		try {
			connection.rollback();
			autodisconnectCoordinator.rollback();
		}
		catch ( Exception e ) {
			throw new ExtensionException("Could not rollback the current transaction.");
		}
	}

	/**
	 * Creates a SqlStatement with the specified parameterized sql statement and
	 * parameters. The SqlStatement is also associated with this SqlConnection.
	 * 
	 * @param sql
	 *            SQL statement (with '?' indicating placeholders for
	 *            parameters)
	 * @param parameters
	 *            Parameters to the query (number of parameters should match the
	 *            number of placeholders in the query)
	 * @return SqlStatement object
	 * @throws SQLException
	 */
	public SqlStatement createStatement(String sql, LogoList parameters)
			throws SQLException {
		LOG.fine("SqlConnection.createStatement('" + sql + "', " + parameters + ")");
		closeStatement();
		PreparedStatement stmt = getConnection().prepareStatement(sql);
		statement = new SqlStatement(stmt, parameters, autodisconnectCoordinator);
		return statement;
	}

	@Override
	public void register(EventObserver<ConnectionEvent> observer) {
		if (!connectionObservers.contains(observer)) {
			LOG.fine("Registering observer " + observer);
			connectionObservers.add(observer);
		}
	}

	@Override
	public void unRegister(EventObserver<ConnectionEvent> observer) {
		connectionObservers.remove(observer);
	}
		
	/**
	 * Coordinator for autodisconnect behavior.
	 * 
	 * @author NetLogo project-team
	 */
	protected class AutodisconnectCoordinator {
		
		/**
		 * Indicate endOfResultSet autodisconnect.
		 */
		protected void endOfResultSet() {
			nonTransaction();
		}
		
		/**
		 * Indicate noResultset autodisconnect
		 */
		protected void noResultSet() {
			nonTransaction();
		}
		
		/**
		 * Non-transactional autodisconnect
		 */
		private void nonTransaction() {
			if (dbInfo.useAutoDisconnect() && autoCommitEnabled()) {
				autoDisconnect();
			}
		}
		
		/**
		 * Indicate commit autodisconnect
		 */
		protected void commit() {
			transaction();
		}
		
		/**
		 * Indicate rollback autodisconnect
		 */
		protected void rollback() {
			transaction();
		}
		
		/**
		 * Transactional autodisconnect
		 */
		private void transaction() {
			if (dbInfo.useAutoDisconnect()) {
				autoDisconnect();
			}
		}
		
	}

	/**
	 * Connection events associated with SqlConnection.
	 * 
	 * @author NetLogo project-team
	 */
	public enum ConnectionEvent {
		/**
		 * Event signifying full closure of both the physical connection to the database and the SqlConnection class.
		 */
		CLOSE,
		/**
		 * Event signifying closure of only the physical connection to the database, the SqlConnection class is retained.
		 */
		AUTO_DISCONNECT
	}
}
