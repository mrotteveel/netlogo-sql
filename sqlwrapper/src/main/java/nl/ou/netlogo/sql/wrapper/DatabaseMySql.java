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

import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoList;

import java.util.logging.Logger;


/**
 * Implements the MySql specific methods that the SQL extension needs to interact with a MySql
 * database engine
 * 
 * @author NetLogo project-team
 *
 */
public class DatabaseMySql extends AbstractDatabase {

	public static final String BRANDNAME = "MySql";
	public static final int DEFAULT_PORT = 3306;
	public static final String JDBC_PATTERN = "jdbc:mysql://%s:%d/%s";
	public static final String DRIVER_CLASSNAME = "com.mysql.jdbc.Driver";
	
	private static final Logger LOG = SqlLogger.getLogger();
	
	public DatabaseMySql(SqlSetting settings) throws Exception {
		super(settings);
	}
	
	public DatabaseMySql(String host, int port, String user, String password, String schemaName) {
		super(host, port, user, password, schemaName);
	}
	
	@Override
	public String getBrandName() {
		return BRANDNAME;
	}
	
	@Override
	public String getDriverClass() {
		return DRIVER_CLASSNAME;
	}
	
	@Override
	public String getJdbcUrl() {
		return String.format(JDBC_PATTERN, getHost(), getPort(), getDatabase());
	}
	
	@Override
	protected int getDefaultPort() {
		return DEFAULT_PORT;
	}
	
	@Override
	public void useDatabase(SqlConnection sqlc, String schemaName) throws ExtensionException {
		try {
			SqlStatement statement = sqlc.createStatement("use " + schemaName);
			// the use <database> will never generate a result set
			// nor a row count, hence no further processing.
			statement.executeDirect();
		}
		catch ( Exception e ) {
			throw new ExtensionException("Could not switch database context to '" + schemaName + "' " + e);
		}
	}


	@Override
	public String getCurrentDatabase(SqlConnection sqlc) {
		if ( sqlc == null ) {
			return ("");
		}
		
		try {
			String DBName = "";
			SqlStatement statement = sqlc.createStatement("select database()");

			if (statement.executeDirect())
			{
				// We have a result, process it.
				SqlResultSet rs = sqlc.getResultSet();
				LogoList result = rs.fetchRow();
				DBName = (String) result.first();
			}
			
			return (DBName);
		}
		// TODO: more meaningful exception handling
		catch (Exception ex) {
			return "";
		}
	}

	@Override
	public boolean findDatabase(SqlConnection sqlc, String schemaName) {
		if ( sqlc != null ) {
			try {
				SqlStatement statement = sqlc.createStatement(
							"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" +
							schemaName + "'");
				if (statement.executeDirect())
				{
					// We have a result, process it.
					SqlResultSet rs = sqlc.getResultSet();
					LogoList result = rs.fetchRow();
					String dbName = (String) result.first();
					return dbName.equalsIgnoreCase(schemaName); 
				}
			}
			catch ( Exception e ) {
				// log, but ignore, semantics is: database not found
				LOG.severe("Exception while finding database '" + schemaName + "': " + e);
			}
		}
		return false;
	}
	
}
