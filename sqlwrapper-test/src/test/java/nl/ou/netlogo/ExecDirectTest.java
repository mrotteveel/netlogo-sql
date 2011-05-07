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

import static nl.ou.netlogo.testsupport.DatabaseHelper.getMySQLConnectCommand;
import static nl.ou.netlogo.testsupport.DatabaseHelper.getMySQLPoolConfigurationCommand;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Calendar;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nlogo.nvm.EngineException;

import nl.ou.netlogo.testsupport.DatabaseHelper;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for behavior of sql:exec-direct, this also checks the effect of resultset-available? for exec-direct queries.
 * <p>
 * Fetching rows and/or resultsets is done as part of {@link FetchRowTest} and {@link FetchResultSetTest}, testing of
 * retrieving rowcount is part of {@link GetRowCountTest}
 * </p>
 * <p>
 * The update cases of sql:exec-direct are only tested with UPDATE statements. Repeating same cases with INSERT and DELETE
 * would be equivalent to testing the underlying JDBC driver.
 * </p> 
 * 
 * @author Mark Rotteveel
 */
public class ExecDirectTest extends HeadlessTest {
	
	private static final String TABLE_PREFIX = "TEST";
	protected static String tableName;

	@BeforeClass
	public static void createTable() throws ClassNotFoundException {
		tableName = TABLE_PREFIX + Calendar.getInstance().getTimeInMillis();

		try {
			DatabaseHelper.executeUpdate("CREATE TABLE " + tableName + "( "
					+ "ID INTEGER PRIMARY KEY, "
					+ "CHAR_FIELD CHAR(25), " 
					+ "INT_FIELD INTEGER, "
					+ "VARCHAR_FIELD VARCHAR(200) " 
					+ ")");
		} catch (SQLException e) {
			throw new IllegalStateException("Unable to setup database", e);
		} 
	}
	
	@Before
	public void setupData() {
		String query = "INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (%d, '%s', %d, '%s')";
		try {
			DatabaseHelper.executeUpdate(
					String.format(query, 1, "CHAR-content", 1234, "VARCHAR-content")
			);
		} catch (SQLException e) {
			throw new IllegalStateException("Unable to setup data", e);
		}
	}
	
	/**
	 * Test if there is a resultset available after executing a select query on a normal connection.
	 * <p>
	 * Expected: sql:resultset-available? returns true after sql:exec-direct with a SELECT.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testSelect_connect_resultsetAvailable() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLConnectCommand());
		
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + "\"");
		
		boolean hasResultSet = (Boolean)workspace.report("sql:resultset-available?");
		assertTrue("Expect exec-direct with select to return a resultset", hasResultSet);
	}
	
	/**
	 * Test if there is no autodisconnect after executing a select query on a normal connection.
	 * <p>
	 * Expected: connection remains open after sql:exec-direct with a SELECT.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testSelect_connect_noAutodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLConnectCommand());
		
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + "\"");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertTrue("Should stay connected after select", isConnected);
	}
	
	/**
	 * Test if there is a resultset available after executing a select query on a pooled connection.
	 * <p>
	 * Expected: sql:resultset-available? returns true after sql:exec-direct with a SELECT.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testSelect_connectionpool_resultsetAvailable() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLPoolConfigurationCommand());
		
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + "\"");
		
		boolean hasResultSet = (Boolean)workspace.report("sql:resultset-available?");
		assertTrue("Expect exec-direct with select to return a resultset", hasResultSet);
	}
	
	/**
	 * Test if there is no autodisconnect after executing a select query on a pooled connection with autodisconnect disabled.
	 * <p>
	 * Expected: connection remains open after sql:exec-direct with a SELECT.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testSelect_connectionPool_noAutodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLPoolConfigurationCommand(false));
		
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + "\"");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertTrue("Should not autodisconnect after select", isConnected);
	}
	
	/**
	 * Test if there is no autodisconnect after executing a select query on a pooled connection with autodisconnect enabled.
	 * <p>
	 * Expected: connection remains open after sql:exec-direct with a SELECT.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testSelect_connectionPool_autodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLPoolConfigurationCommand());
		
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + "\"");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertTrue("Should not autodisconnect after select", isConnected);
	}
	
	/**
	 * Test if an UPDATE query executed on a normal connection does not have a resultset available.
	 * <p>
	 * Expected: sql:resultset-available returns false after sql:exec-direct with UPDATE statement.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testUpdate_connect_resultsetAvailable() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLConnectCommand());
		
		workspace.command("sql:exec-direct \"UPDATE " + tableName + " SET INT_FIELD = 2345 WHERE ID = 1\"");
		
		boolean hasResultSet = (Boolean)workspace.report("sql:resultset-available?");
		assertFalse("Expect exec-direct with update statement to return false", hasResultSet);
	}
	
	/**
	 * Test if an UPDATE query executed on a normal connection does not autodisconnect.
	 * <p>
	 * Expected: connection remains open after sql:exec-direct with UPDATE.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testUpdate_connect_noAutodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLConnectCommand());
		
		workspace.command("sql:exec-direct \"UPDATE " + tableName + " SET INT_FIELD = 2345 WHERE ID = 1\"");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertTrue("Should stay connected after update", isConnected);
	}
	
	/**
	 * Test if an UPDATE query executed on a pooled connection does not have a resultset available.
	 * <p>
	 * Expected: sql:resultset-available returns false after sql:exec-direct with UPDATE statement.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testUpdate_connectionPool_resultsetAvailable() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLConnectCommand());
		
		workspace.command("sql:exec-direct \"UPDATE " + tableName + " SET INT_FIELD = 2345 WHERE ID = 1\"");
		
		boolean hasResultSet = (Boolean)workspace.report("sql:resultset-available?");
		assertFalse("Expect exec-direct with update statement to return false", hasResultSet);
	}
	
	/**
	 * Test if an UPDATE query executed on a pooled connection with autodisconnect disabled does not autodisconnect.
	 * <p>
	 * Expected: connection remains open after sql:exec-direct with UPDATE.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testUpdate_connectionPool_NoAutodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLPoolConfigurationCommand(false));
		
		workspace.command("sql:exec-direct \"UPDATE " + tableName + " SET INT_FIELD = 2345 WHERE ID = 1\"");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertTrue("Should stay connected after update", isConnected);
	}
	
	/**
	 * Test if an UPDATE query executed on a pooled connection with autodisconnect enabled does autodisconnect.
	 * <p>
	 * Expected: connection is closed after sql:exec-direct with UPDATE.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testUpdate_connectionPool_autoDisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLPoolConfigurationCommand());
		
		workspace.command("sql:exec-direct \"UPDATE " + tableName + " SET INT_FIELD = 2345 WHERE ID = 1\"");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertFalse("Should be auto-disconnected after update", isConnected);
		
		boolean hasResultSet = (Boolean)workspace.report("sql:resultset-available?");
		assertFalse("Expect resultset-available after autodisconnect to return false", hasResultSet);
		assertEquals("Unexpected rowcount after autodisconnect", Double.valueOf(1), workspace.report("sql:get-rowcount"));
	}
	
	// Repeating update-cases for INSERT and DELETE is testing the JDBC interface, not the plugin.
	
	/**
	 * Test if using invalid syntax in query throws an exception.
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test(expected=EngineException.class)
	public void testSyntaxError() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLConnectCommand());
		
		workspace.command("sql:exec-direct \"SLECT * FROM " + tableName + "\"");
	}
	
	@After
	public void teardownData() {
		try {
			DatabaseHelper.executeUpdate("TRUNCATE TABLE " + tableName);
		} catch (SQLException e) {
			throw new IllegalStateException("Unable to delete data", e);
		}
	}

	@AfterClass
	public static void dropTable() {
		try {
			DatabaseHelper.executeUpdate(new String[] {"DROP TABLE " + tableName});
		} catch (SQLException e) {
			throw new IllegalStateException("Unable to drop table " + tableName, e);
		} 
	}

}
