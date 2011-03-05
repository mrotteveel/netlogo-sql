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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.Collections;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nlogo.api.LogoList;

import nl.ou.netlogo.testsupport.DatabaseHelper;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for the sql:fetch-resultset reporter.
 * <p>
 * Because of the structure of the tests some parts of sql:fetch-resultset are tested in 
 * {@link ExecDirectTest} and {@link ExecQueryTest}.
 * </p>
 * 
 * @author Mark Rotteveel
 */
public class FetchResultSetTest extends HeadlessTest {
	
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
	 * Test basic behavior of sql:fetch-resultset.
	 * <p>
	 * Expected: resultset contains 4 rows, values in rows match expected values.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testFetchResultSet() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
		
		LogoList rows = (LogoList)workspace.report("sql:fetch-resultset");
		assertEquals("Unexpected resultset size", 4, rows.size());
		for (int i = 1; i <= 4; i++) {
			LogoList row = (LogoList)rows.get(i - 1);
			assertEquals(Double.valueOf(i), row.get(0));
			assertEquals("CHAR-content", row.get(1));
			assertEquals(Double.valueOf(1234), row.get(2));
			assertEquals("VARCHAR-content", row.get(3));
		}
	}
	
	/**
	 * Test behavior of sql:fetch-resultset for empty resultset.
	 * <p>
	 * Expected: resultset contains 0 rows.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testFetchResultSet_emptyResult() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " WHERE 1 = 0\"");
		
		LogoList rows = (LogoList)workspace.report("sql:fetch-resultset");
		assertEquals("Expected empty list for empty resultset", 0, rows.size());
	}
	
	/**
	 * Test if a connection made with sql:connect is not autodisconnected after fetching the resultset.
	 * <p>
	 * Expected: connection remains open after sql:fetch-resultset.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testFetchResultset_connect_noAutodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
		
		workspace.report("sql:fetch-resultset");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertTrue("Should stay connected after fetch-resultset", isConnected);
	}
	
	/**
	 * Test if a connection made from connectionpool is not autodisconnected after fetching the resultset
	 * when autodisconnect is disabled.
	 * <p>
	 * Expected: connection remains open after sql:fetch-resultset.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testFetchResultset_connectionPool_noAutodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand(false));
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
		
		workspace.report("sql:fetch-resultset");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertTrue("Should stay connected after fetch-resultset", isConnected);
	}
	
	/**
	 * Test if a connection made from connectionpool is autodisconnected after fetching the resultset
	 * when autodisconnect is enabled.
	 * <p>
	 * Expected: connection is closed after sql:fetch-resultset.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testFetchResultset_connectionPool_autodisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertTrue("Should stay connected after select", isConnected);
		
		workspace.report("sql:fetch-resultset");
		
		isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertFalse("Should be auto-disconnected after fetch-resultset", isConnected);
	}
	
	/**
	 * Test if sql:fetch-resultset returns an empty logolist if called without a connection.
	 * <p>
	 * Expected: empty list of type LogoList
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testFetchResultSet_noConnection() throws Exception {
		workspace.open("init-sql.nlogo");
		
		Object resultset = workspace.report("sql:fetch-resultset");
		
		assertTrue("Expected resultset of type LogoList", resultset instanceof LogoList);
		assertEquals("Expected emptylist as a result", Collections.EMPTY_LIST, resultset);
	}
	
	/**
	 * Test if sql:fetch-resultset returns an empty logolist if called with a connection using sql:connect, but without a statement.
	 * <p>
	 * Expected: empty list of type LogoList
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testFetchResultSet_connect_noStatement() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		
		Object resultset = workspace.report("sql:fetch-resultset");
		
		assertTrue("Expected resultset of type LogoList", resultset instanceof LogoList);
		assertEquals("Expected emptylist as a result", Collections.EMPTY_LIST, resultset);
	}
	
	/**
	 * Test if sql:fetch-resultset returns an empty logolist if called with the connectionpool, but without a statement.
	 * <p>
	 * Expected: empty list of type LogoList
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testFetchResultSet_connectionpool_noStatement() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		
		Object resultset = workspace.report("sql:fetch-resultset");
		
		assertTrue("Expected resultset of type LogoList", resultset instanceof LogoList);
		assertEquals("Expected emptylist as a result", Collections.EMPTY_LIST, resultset);
	}
	
	/**
	 * Test if sql:fetch-resultset returns an empty logolist if called after an update statement.
	 * <p>
	 * Expected: empty list of type LogoList
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testFetchResultSet_updateQuery() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"UPDATE " + tableName + "  SET INT_FIELD = 5\"");
		
		Object resultset = workspace.report("sql:fetch-resultset");
		
		assertTrue("Expected resultset of type LogoList", resultset instanceof LogoList);
		assertEquals("Expected emptylist as a result", Collections.EMPTY_LIST, resultset);
	}
	
	// TODO Add tests for fetching twice, cover more datatypes
	
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
