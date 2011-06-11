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
package nl.ou.netlogo;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;
import org.nlogo.api.CompilerException;
import org.nlogo.api.LogoException;
import org.nlogo.nvm.EngineException;

import nl.ou.netlogo.testsupport.Database;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for sql:configure.
 * <p>
 * These tests do not cover all aspects that are configurable. The connectionpool aspect will
 * be tested as part of {@link ConnectionPoolTest}. The logging aspect(s) will not be tested
 * through test-automation.
 * </p>
 * 
 * @author Mark Rotteveel
 */
public class ConfigureTest extends HeadlessTest {
	
	/**
	 * Test if the defaultconnection aspect accepts all parameters.
	 * <p>
	 * Expected: parameters accepted and a connection can be created.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConfigure_MySQL_defaultconnection_allParameters() throws Exception {
		workspace.open("init-sql.nlogo");
		
		Database db = Database.MYSQL;
		workspace.command(
				String.format("sql:configure \"defaultconnection\" [[\"brand\" \"MySQL\"] [\"host\" \"%s\"] [\"port\" %s] [\"user\" \"%s\"] " +
						"[\"password\" \"%s\"] [\"database\" \"%s\"] [\"autodisconnect\" \"on\"]]",
				db.getHost(), db.getPort(), db.getUsername(), db.getPassword(), db.getSchema()));
		String observerDB = (String)workspace.report("sql:current-database");
		assertEquals("Unexpected connection database", db.getSchema(), observerDB);
	}
	
	/**
	 * Test if the port parameter for defaultconnection aspect is optional.
	 * <p>
	 * Expected: parameters accepted and a connection can be created.
	 * </p>
	 * <p>
	 * Implicitly also checks if brand and autodisconnect are optional
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConfigure_MySQL_defaultconnection_minusPort() throws IOException, CompilerException, LogoException {
		workspace.open("init-sql.nlogo");
		
		Database db = Database.MYSQL;
		workspace.command(
				String.format("sql:configure \"defaultconnection\" [[\"host\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"]]",
				db.getHost(), db.getUsername(), db.getPassword(), db.getSchema()));
		String observerDB = (String)workspace.report("sql:current-database");
		assertEquals("Unexpected connection database", db.getSchema(), observerDB);
	}
	
	/**
	 * Test if the host parameter for defaultconnection aspect is optional.
	 * <p>
	 * Expected: parameters accepted and a connection can be created.
	 * </p>
	 * <p>
	 * Implicitly also checks if brand and autodisconnect are optional
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConfigure_MySQL_defaultconnection_minusHost() throws Exception {
		workspace.open("init-sql.nlogo");
		
		Database db = Database.MYSQL;
		workspace.command(
				String.format("sql:configure \"defaultconnection\" [[\"port\" %s] [\"user\" \"%s\"] [\"password\" \"%s\"] " +
						"[\"database\" \"%s\"]]",
				db.getPort(), db.getUsername(), db.getPassword(), db.getSchema()));
		String observerDB = (String)workspace.report("sql:current-database");
		assertEquals("Unexpected connection database", db.getSchema(), observerDB);
	}
	
	/**
	 * Test if an unknown setting is not supported.
	 * <p>
	 * Expected: throws exception
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test(expected = EngineException.class)
	public void testConfigure_unknownSetting() throws Exception {
		workspace.open("init-sql.nlogo");
		
		workspace.command("sql:configure \"unknownsetting\" []");
	}
	
	/**
	 * Test if passing an unknown parameter to a setting is not supported.
	 * <p>
	 * Expected: throws exception
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test(expected=EngineException.class)
	public void testConfigure_unknownParameter() throws Exception {
		workspace.open("init-sql.nlogo");
		Database db = Database.MYSQL;;
		workspace.command(
				String.format("sql:configure \"defaultconnection\" [[\"unknownsetting\" \"value\"] [\"port\" %s] [\"user\" \"%s\"] " +
						"[\"password\" \"%s\"] [\"database\" \"%s\"]]",
				db.getPort(), db.getUsername(), db.getPassword(), db.getSchema()));
	}
}
