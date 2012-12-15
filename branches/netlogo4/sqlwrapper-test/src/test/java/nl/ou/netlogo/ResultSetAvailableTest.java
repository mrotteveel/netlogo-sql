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

import static nl.ou.netlogo.testsupport.DatabaseHelper.getDefaultPoolConfigurationCommand;
import static nl.ou.netlogo.testsupport.DatabaseHelper.getDefaultConnectCommand;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Test for resultset-available?
 * <p>
 * Tests directly related to executing queries are part of {@link ExecDirectTest}, {@link ExecQueryTest} and {@link ExecUpdateTest}; this
 * test is only for other testcases.
 * </p>
 * @author Mark Rotteveel
 *
 */
public class ResultSetAvailableTest extends HeadlessTest {
	
	/**
	 * Test if sql:resultset-available? returns false if called without a connection.
	 * <p>
	 * Expected: sql:resultset-available? returns false
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testResultsetAvailable_noConnection() throws Exception {
		workspace.open("init-sql.nlogo");
		
		assertFalse("Expected false for resultset-available? without connection", (Boolean)workspace.report("sql:resultset-available?"));
	}
	
	/**
	 * Test if sql:resultset-available? returns false if called with connectionpooling, without an executed statement.
	 * <p>
	 * Expected: sql:resultset-available? returns false
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testResultsetAvailable_connectionPool_noStatement() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		
		assertFalse("Expected false for resultset-available? without connection", (Boolean)workspace.report("sql:resultset-available?"));
	}
	
	/**
	 * Test if sql:resultset-available? returns false if called with a connection, without an executed statement.
	 * <p>
	 * Expected: sql:resultset-available? returns false
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testResultSetAvailable_noStatement() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultConnectCommand());
		Boolean hasResultset = (Boolean)workspace.report("sql:resultset-available?");
		assertFalse("Unexpected resultset availability: expected no resultset", hasResultset);
	}

}
