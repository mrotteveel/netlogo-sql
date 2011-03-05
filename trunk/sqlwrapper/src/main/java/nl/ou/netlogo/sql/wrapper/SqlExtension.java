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

import nl.ou.netlogo.sql.extension.*;

import org.nlogo.api.*;

import java.util.logging.*;

/**
 * ClassManager for the sql extension for NetLogo.
 * 
 * @author NetLogo 2010 team
 */
public class SqlExtension extends DefaultClassManager {

	public static final String DEBUG_IS_CONNECTED = "debug-is-connected?";
	public static final String GET_ROWCOUNT = "get-rowcount";
	public static final String FETCH_RESULTSET = "fetch-resultset";
	public static final String FETCH_ROW = "fetch-row";
	public static final String ROW_AVAILABLE = "row-available?";
	public static final String RESULTSET_AVAILABLE = "resultset-available?";
	public static final String EXEC_UPDATE = "exec-update";
	public static final String EXEC_QUERY = "exec-query";
	public static final String EXEC_DIRECT = "exec-direct";
	public static final String LOG = "log";
	public static final String GET_FULL_CONFIGURATION = "get-full-configuration";
	public static final String GET_CONFIGURATION = "get-configuration";
	public static final String CONFIGURE = "configure";
	public static final String SHOW_VERSION = "show-version";
	public static final String CONNECT = "connect";
	public static final String DISCONNECT = "disconnect";
	public static final String IS_CONNECTED = "is-connected?";
	public static final String USE_DATABASE = "use-database";
	public static final String CURRENT_DATABASE = "current-database";
	public static final String FIND_DATABASE = "find-database";
	public static final String AUTOCOMMITON = "autocommit-on";
	public static final String AUTOCOMMITOFF = "autocommit-off";
	public static final String AUTOCOMMIT_ENABLED = "autocommit-enabled?";
	public static final String START_TRANSACTION = "start-transaction";
	public static final String COMMIT_TRANSACTION = "commit-transaction";
	public static final String ROLLBACK_TRANSACTION = "rollback-transaction";
	
	//
	// Initialize the environment. When the model compiles,
	// the load() method is called once. All environment defaults
	// are available. The environment is shared across agents.
	//
	static SqlEnvironment sqlenv = null;
	private static SqlLogger sqlLogger = null;
	
	static {
		try {
			sqlLogger = new SqlLogger();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void load(PrimitiveManager primitiveManager)
			throws ExtensionException {
		SqlLogger.getLogger().log(Level.FINE, "Call to load");
		// Other
		primitiveManager.addPrimitive(SHOW_VERSION, new ShowVersion());

		// Connection setup and maintenance
		primitiveManager.addPrimitive(CONNECT, new Connect());
		primitiveManager.addPrimitive(IS_CONNECTED, new IsConnected());
		primitiveManager.addPrimitive(DISCONNECT, new Disconnect());
		primitiveManager.addPrimitive(USE_DATABASE, new UseDatabase());
		primitiveManager.addPrimitive(CURRENT_DATABASE, new CurrentDatabase());
		primitiveManager.addPrimitive(FIND_DATABASE, new FindDatabase());
		
		// Transaction management
		primitiveManager.addPrimitive(AUTOCOMMITON, new AutoCommitOn());
		primitiveManager.addPrimitive(AUTOCOMMITOFF, new AutoCommitOff());
		primitiveManager.addPrimitive(AUTOCOMMIT_ENABLED, new AutoCommitEnabled());
		primitiveManager.addPrimitive(START_TRANSACTION, new StartTransaction());
		primitiveManager.addPrimitive(COMMIT_TRANSACTION, new CommitTransaction());
		primitiveManager.addPrimitive(ROLLBACK_TRANSACTION, new RollbackTransaction());
		
		// Configuration
		primitiveManager.addPrimitive(CONFIGURE, new Configure());
		primitiveManager.addPrimitive(GET_CONFIGURATION, new GetConfiguration());
		primitiveManager.addPrimitive(GET_FULL_CONFIGURATION, new GetFullConfiguration());

		// Logging
		primitiveManager.addPrimitive(LOG, new Log());
		
		// Query
		primitiveManager.addPrimitive(EXEC_DIRECT, new ExecDirect());
		primitiveManager.addPrimitive(EXEC_QUERY, new ExecQuery());
		primitiveManager.addPrimitive(EXEC_UPDATE, new ExecUpdate());

		// Result processing
		primitiveManager.addPrimitive(RESULTSET_AVAILABLE, new ResultSetAvailable());
		primitiveManager.addPrimitive(ROW_AVAILABLE, new RowAvailable());
		primitiveManager.addPrimitive(FETCH_ROW, new FetchRow());
		primitiveManager.addPrimitive(FETCH_RESULTSET, new FetchResultSet());
		primitiveManager.addPrimitive(GET_ROWCOUNT, new GetRowCount());
		
		// Debugging/testing
		primitiveManager.addPrimitive(DEBUG_IS_CONNECTED, new IsConnectedDebug());
	}
	
	@Override
	public void unload() throws ExtensionException {
		SqlLogger.getLogger().info("Call to SqlExtension.unload()");
		SqlConnectionManager conMan = getSqlEnvironment().getConnectionManager();
		conMan.closeAll();
		conMan.shutdownConnectionPool();
		
		super.unload();
	}

	/**
	 * Method used to return the sql environment handle.
	 * 
	 * @return sql environment handle
	 */
	public static SqlEnvironment getSqlEnvironment() {
		if (sqlenv == null) {
			SqlLogger.getLogger().log(Level.FINE,"Creating new SqlEnvironment");
			sqlenv = new SqlEnvironment();

			//
			// register objects that are configurable with the SqlConfiguration
			// class in sqlenv
			//
			try {
				// NB: Logging should be configured FIRST, so that the default logging settings
				// will be obeyed while configuring other aspects
				sqlenv.getConfiguration().addConfigurable(SqlConfiguration.LOGGING, sqlLogger);
				sqlenv.getConfiguration().addConfigurable(SqlConfiguration.DEFAULTCONNECTION,
						sqlenv.getConnectionManager());
				sqlenv.getConfiguration().addConfigurable(SqlConfiguration.CONNECTIONPOOL,
						sqlenv.getConnectionManager());
			} catch (Exception ex) {
				throw new IllegalStateException("Unable to initialize SqlEnvironment: ", ex);
			}
		}

		return sqlenv;
	}
}
