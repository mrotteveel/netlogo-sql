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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.nlogo.api.ExtensionException;

/**
 * Test for (aspects off) {@link AbstractDatabase}.
 * 
 * @author NetLogo project-team
 */
public class AbstractDatabaseTest {
	
	private static final int TEST_DEFAULT_PORT = 31415;
	
    /**
     * Test implementation of AbstractDatabase.
     * 
     * @author NetLogo project-team
     */
	private static class TestDatabase extends AbstractDatabase {
		
		TestDatabase(String host, int port, String user, String password, String schema) {
			super(host, port, user, password, schema);
		}

		@Override
		public String getJdbcUrl() { return null; }

		@Override
		public String getDriverClass() { return null; }

		@Override
		public void useDatabase(SqlConnection conn, String schemaName)
				throws ExtensionException { }

		@Override
		public String getCurrentDatabase(SqlConnection conn) { return null; }

		@Override
		public boolean findDatabase(SqlConnection sqlc, String schemaName) { return false; }

		@Override
		public String getBrandName() { return null; }

		@Override
		protected int getDefaultPort() { 
			return TEST_DEFAULT_PORT;
		}
	}
	
	/**
	 * When the port is configured, {@link AbstractDatabase#getPort()} should return that port.
	 */
	@Test
	public void test_getPort() {
		int configuredPort = 1234;
		TestDatabase db = new TestDatabase("host", configuredPort, "user", "password", "schema");
		
		
		
		assertEquals("Expected configured port value to be returned by getPort()", configuredPort, db.getPort());
	}

	/**
	 * If the configured port is 0, {@link AbstractDatabase#getPort()} should return the defaultPort returned
	 * by the implementation of {@link AbstractDatabase#getDefaultPort()}.
	 */
	@Test
	public void test_getPort_default() {
		TestDatabase db = new TestDatabase("host", 0, "user", "password", "schema");
		
		assertEquals("Expected default port to be returned by getPort()", TEST_DEFAULT_PORT, db.getPort());
	}
}
