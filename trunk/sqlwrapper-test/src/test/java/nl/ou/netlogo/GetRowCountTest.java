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
 * Tests for the sql:get-rowcount reporter.
 * <p>
 * Note that update related tests are only tested with UPDATE statements, testing the same using INSERT and DELETE
 * would be testing the JDBC driver.
 * </p>
 * 
 * @author Mark Rotteveel
 */
public class GetRowCountTest extends HeadlessTest {
	
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
	 * Test if sql:get-rowcount works after executing a SELECT statement using sql:exec-direct.
	 * <p>
	 * Expected: returned rowcount is 0 when performing SELECTS.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testSelect_ExecDirect_rowCount() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + "\"");
		
		Object rowCount = workspace.report("sql:get-rowcount");
		
		assertEquals("Unexpected rowcount", Double.valueOf(-1), rowCount);
	}
	
	/**
	 * Test if sql:get-rowcount works after executing a SELECT statement using sql:exec-query.
	 * <p>
	 * Expected: returned rowcount is -1 when performing SELECTS.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testSelect_ExecQuery_rowCount() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-query \"SELECT * FROM " + tableName + "\" []");
		
		Object rowCount = workspace.report("sql:get-rowcount");
		
		assertEquals("Unexpected rowcount", Double.valueOf(-1), rowCount);
	}
	
	/**
	 * Test if sql:get-rowcount works after executing an UPDATE statement using sql:exec-direct.
	 * <p>
	 * Expected: returned rowcount is 1 when performing UPDATE on single row.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testUpdate_ExecDirect_rowCount() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-direct \"UPDATE " + tableName + " SET INT_FIELD = 2345 WHERE ID = 1\"");
		
		Object rowCount = workspace.report("sql:get-rowcount");
		
		assertEquals("Unexpected rowcount", Double.valueOf(1), rowCount);
	}
	
	/**
	 * Test if sql:get-rowcount works after executing an UPDATE statement using sql:exec-update.
	 * <p>
	 * Expected: returned rowcount is 1 when performing UPDATE on single row.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testUpdate_ExecUpdate_rowCount() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		workspace.command("sql:exec-update \"UPDATE " + tableName + " SET INT_FIELD = 2345 WHERE ID = 1\" []");
		
		Object rowCount = workspace.report("sql:get-rowcount");
		assertEquals("Unexpected rowcount", Double.valueOf(1), rowCount);
	}
	
	/**
	 * Test if sql:get-rowcount works after executing an UPDATE statement using sql:exec-update, when connected with pooling with
	 * autodisconnect enabled.
	 * <p>
	 * Expected: returned rowcount is 1 when performing UPDATE on single row.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testUpdate_autodisconnect_rowCount() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		workspace.command("sql:exec-update \"UPDATE " + tableName + " SET INT_FIELD = 2345 WHERE ID = ?\" [1]");
		
		boolean isConnected = (Boolean)workspace.report("sql:debug-is-connected?");
		assertFalse("Expected to be autodisconnected", isConnected);
		
		Object rowCount = workspace.report("sql:get-rowcount");
		assertEquals("Unexpected rowcount", Double.valueOf(1), rowCount);
	}
	
	/**
	 * Test if sql:get-rowcount returns -1.0 if executed without a connection.
	 * <p>
	 * Expected: return value is -1 (of type Double).
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testGetRowCount_noConnection() throws Exception {
		workspace.open("init-sql.nlogo");
		
		Object result = workspace.report("sql:get-rowcount");
		
		assertEquals("Unexpected rowcount", Double.valueOf(-1), result);
	}
	
	/**
	 * Test if sql:get-rowcount returns -1 if a connection is created with sql:connect, but no statement
	 * was executed.
	 * <p>
	 * Expected: Returned rowcount is -1
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testGetRowCount_connect_noStatement() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultSqlConnectCommand());
		
		Object rowCount = (Double)workspace.report("sql:get-rowcount");
		
		assertEquals("Unexpected rowcount without statement", Double.valueOf(-1), rowCount);
	}
	
	/**
	 * Test if sql:get-rowcount returns -1 if executed with connectionpooling, without a statement.
	 * <p>
	 * Expected: Returned rowcount is -1
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testGetRowCount_connectionPool_noStatement() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		
		Object result = workspace.report("sql:get-rowcount");
		
		assertEquals("Unexpected rowcount", Double.valueOf(-1), result);
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
