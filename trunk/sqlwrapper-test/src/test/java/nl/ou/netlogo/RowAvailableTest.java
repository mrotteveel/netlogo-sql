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
import static nl.ou.netlogo.testsupport.DatabaseHelper.getDefaultSqlConnectCommand;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Calendar;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import nl.ou.netlogo.testsupport.DatabaseHelper;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for sql:row-available? reporter.
 *  
 * @author Mark Rotteveel
 */
public class RowAvailableTest extends HeadlessTest {
	
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
					String.format(query, 1, "CHAR-content", 1234, "VARCHAR-content"),
					String.format(query, 2, "CHAR-content", 1234, "VARCHAR-content"),
					String.format(query, 3, "CHAR-content", 1234, "VARCHAR-content"),
					String.format(query, 4, "CHAR-content", 1234, "VARCHAR-content")
			);
		} catch (SQLException e) {
			throw new IllegalStateException("Unable to setup data", e);
		}
	}
	
	/**
	 * Test if sql:row-available? returns false when called without a connection.
	 * <p>
	 * Expected: returns false for no connection.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_noConnection() throws Exception {
		workspace.open("init-sql.nlogo");
		assertFalse("Expected false for row-available? without connection", (Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? returns false when called with connectionpooling, without an executed statement.
	 * <p>
	 * Expected: returns false for no connection.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_connectionPool_noStatement() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		workspace.report("sql:row-available?");
		assertFalse("Expected false for row-available? with connection pooling without statement", 
				(Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? returns false when called with a connection, without an executed statement.
	 * <p>
	 * Expected: sql:row-available? returns false
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_connect_noStatement() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		
		assertFalse("row-available? for no query should return false", (Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? returns true when called first time with a non-empty resultset.
	 * <p>
	 * Expected: sql:row-available? returns true.
	 * </p>
	 *  
	 * @throws Exception For any exceptions during testing
	 */
	@Test 
	public void testRowAvailable_nonEmptyFirst() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
		
		assertTrue("Non-empty result should return true for first row-available?", (Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? returns true when called with a non-empty resultset created using sql:exec-query.
	 * <p>
	 * Expected: sql:row-available? returns true.
	 * </p>
	 *  
	 * @throws Exception For any exceptions during testing
	 */
	@Test 
	public void testRowAvailable_ExecQuery() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-query \"SELECT * FROM " + tableName + " ORDER BY ID\" []");
		
		assertTrue("Non-empty result should return true for first row-available?", (Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? returns false when called first time with an empty resultset.
	 * <p>
	 * Expected: sql:row-available? returns false.
	 * </p>
	 *  
	 * @throws Exception For any exceptions during testing
	 */
	@Test 
	public void testRowAvailable_emptyFirst() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " WHERE 1 = 0\"");
		
		assertFalse("Empty result should return false for row-available?", (Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? returns true as long as the next sql:fetch-row will return a new row.
	 * <p>
	 * Expected: sql:row-available? returns true as long as there are rows and false after the last row is returned.
	 * </p>
	 *  
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_fourFetchRows() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
		for (int row = 1; row <= 4; row++) {
			assertTrue(String.format("row-available? before row %s should be true", row), (Boolean)workspace.report("sql:row-available?"));
			workspace.report("sql:fetch-row");
		}
		
		assertFalse("row-available after row 4 should be false", (Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? returns false after a sql:fetch-resultset.
	 * <p>
	 * Expected: sql:row-available? returns false after a sql:fetch-resultset.
	 * </p>
	 *  
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_FetchResultSet() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
		
		assertTrue("row-available? before fetch-resultset should be true", (Boolean)workspace.report("sql:row-available?"));
		workspace.report("sql:fetch-resultset");
		
		assertFalse("row-available after fetch-resultset should be false", (Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? returns false after an autodisconnect.
	 * <p>
	 * Expected: sql:row-available? returns false.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_after_autodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
		workspace.report("sql:fetch-resultset");
		
		assertFalse("Expected to be autodisconnected", (Boolean)workspace.report("sql:debug-is-connected?"));
		assertFalse("row-available after autodisconnect should be false", (Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? does not perform an autodisconnect on a connection created using sql:connect.
	 * <p>
	 * Expected: connection is kept open.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_connect_EmptyResult_noAutodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " WHERE 1 = 0\"");
		
		assertFalse("row-available on empty result should be false", (Boolean)workspace.report("sql:row-available?"));
		assertTrue("Expected to be remain connected", (Boolean)workspace.report("sql:debug-is-connected?"));
	}
	
	/**
	 * Test if sql:row-available? does not perform an autodisconnect on a connectionpool with autodisconnect disabled
	 * <p>
	 * Expected: connection is kept open.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_connectionpool_EmptyResult_noAutodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand(false));
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " WHERE 1 = 0\"");
		
		assertFalse("row-available on empty result should be false", (Boolean)workspace.report("sql:row-available?"));
		assertTrue("Expected to remain connected", (Boolean)workspace.report("sql:debug-is-connected?"));
	}
	
	/**
	 * Test if sql:row-available? does perform an autodisconnect on a connectionpool with autodisconnect enabled.
	 * <p>
	 * Expected: connection is kept open.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_connectionpool_EmptyResult_autodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " WHERE 1 = 0\"");
		
		assertFalse("row-available on empty result should be false", (Boolean)workspace.report("sql:row-available?"));
		assertFalse("Expected to be disconnected", (Boolean)workspace.report("sql:debug-is-connected?"));
	}
	
	/**
	 * Test if sql:row-available? returns false when an update statement has been executed (DELETE in this test).
	 * <p>
	 * Expected: sql:row-available? returns false
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_updateStatement() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"DELETE FROM " + tableName + "\"");
		
		assertFalse("row-available after update statement(delete) should be false", (Boolean)workspace.report("sql:row-available?"));
	}
	
	/**
	 * Test if sql:row-available? returns false when an update statement has been executed with sql:exec-update (DELETE in this test).
	 * <p>
	 * Expected: sql:row-available? returns false
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testRowAvailable_ExecUpdate() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-update \"DELETE FROM " + tableName + "\" []");
		
		assertFalse("row-available after update statement(delete) should be false", (Boolean)workspace.report("sql:row-available?"));
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
