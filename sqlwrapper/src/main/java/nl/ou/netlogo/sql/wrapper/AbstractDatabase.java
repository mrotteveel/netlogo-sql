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

import org.nlogo.api.Context;

/**
 * Abstract base class for implementing database engine dependent features.
 * Database engine dependent classes must implement this, and the DatabaseInfo interface.
 * 
 * @see nl.ou.netlogo.sql.wrapper.DatabaseInfo
 * @author NetLogo project-team
 *
 */
public abstract class AbstractDatabase implements DatabaseInfo, SqlConfigurable {
	private String host;
	private int port;
	private String user;
	private String password;
	private String schemaName;
	private boolean useAutoDisconnect = false;
	private boolean isConfigured = false;
	
	/**
	 * Convenience constructor to use an SqlSetting object to initialize this object
	 * 
	 * @param settings should contain settings to make the database engine dependent object fully configured
	 * @throws Exception
	 */
	protected AbstractDatabase(SqlSetting settings) throws Exception {
		configure(settings, null);
	}
	
	/**
	 * Initializes a database engine dependent class from the basic required values
	 * 
	 * @param host host for the database engine
	 * @param port network port for the database engine
	 * @param user user to access the database engine
	 * @param password password to access the database engine
	 * @param schemaName schema/database name to access
	 */
	protected AbstractDatabase(String host, int port,
							String user, String password, String schemaName) {
		this.host = host;
		this.port = port;
		this.user = user;
		this.password = password;
		this.schemaName = schemaName;
		this.isConfigured = true;
	}

	@Override
	public void configure(SqlSetting settings, Context context)
			throws Exception {
		if (settings.isValid()) {
			host = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_HOST);
			port = settings.getInt(SqlConfiguration.DEFAULTCONNECTION_OPT_PORT);
			user = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_USER);
			password = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_PASSWORD);
			schemaName = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DATABASE);
			useAutoDisconnect = SqlSetting.toggleValue(settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_AUTODISCONNECT));
			isConfigured = true;
		}
	}
	
	@Override
	public boolean isConfigured() {
		return isConfigured;
	}
	
	@Override
	public String getHost() {
		return host;
	}

	@Override
	public int getPort() {
		return port != 0 ? port : getDefaultPort();
	}
	
	/**
	 * @return Default port for this database.
	 */
	protected abstract int getDefaultPort();
	
	@Override
	public String getUser() {
		return user;
	}
	
	@Override
	public String getPassword() {
		return password;
	}
	
	@Override
	public String getDatabase() {
		return schemaName;
	}
	
	@Override
	public void setAutoDisconnect(boolean toggle) {
		this.useAutoDisconnect = toggle;
	}
	
	@Override
	public boolean useAutoDisconnect() {
		return useAutoDisconnect;
	}
}
