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
package nl.ou.netlogo.testsupport;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Returns the ConnectionInformation specified in the test-connection.properties file, or overridden
 * using the system properties db.host, db.port, db.schema, db.username or db.password.
 *  
 * @author Mark
 */
public class ConnectionInformation {
	
	public static final String HOST_PROPERTY = "db.host";
	public static final String PORT_PROPERTY = "db.port";
	public static final String SCHEMA_PROPERTY = "db.schema";
	public static final String USERNAME_PROPERTY = "db.username";
	public static final String PASSWORD_PROPERTY = "db.password";
	public static final String AUTODISCONNECT_PROPERTY = "db.autodisconnect";
	public static final String MAXCONNECTIONS_PROPERTY = "db.maxconnections";

	private static ConnectionInformation instance;
	private Properties defaultConnectionProperties;
	
	private ConnectionInformation(Properties defaultConnectionProperties) {
		this.defaultConnectionProperties = defaultConnectionProperties;
	}
	
	public String getHost() {
		return getProperty(HOST_PROPERTY);
	}
	
	public String getPort() {
		return getProperty(PORT_PROPERTY);
	}
	
	public String getSchema() {
		return getProperty(SCHEMA_PROPERTY);
	}
	
	public String getUsername() {
		return getProperty(USERNAME_PROPERTY);
	}
	
	public String getPassword() {
		return getProperty(PASSWORD_PROPERTY);
	}
	
	public String getAutoDisconnect() {
		return getProperty(AUTODISCONNECT_PROPERTY);
	}
	
	private String getProperty(String name) {
		return System.getProperty(name, defaultConnectionProperties.getProperty(name));
	}

	/**
	 * 
	 * @return Singleton instance of ConnectionInformation
	 */
	public static ConnectionInformation getInstance() {
		if (instance == null) {
			InputStream is = ConnectionInformation.class
					.getResourceAsStream("/test-connection.properties");
			Properties props = new Properties();
			try {
				props.load(is);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (is != null) {
					try {
						is.close();
					} catch (IOException e) {
						// ignore
					}
				}
			}
			instance = new ConnectionInformation(props);
		}
		return instance;
	}

}
