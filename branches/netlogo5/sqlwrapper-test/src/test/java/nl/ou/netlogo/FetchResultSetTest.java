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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.nlogo.api.LogoList;

import nl.ou.netlogo.testsupport.Database;
import nl.ou.netlogo.testsupport.DatabaseHelper;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for the sql:fetch-resultset reporter for all {@link Database} values.
 * <p>
 * Because of the structure of the tests some parts of sql:fetch-resultset are
 * tested in {@link ExecDirectTest} and {@link ExecQueryTest}.
 * </p>
 * 
 * @author Mark Rotteveel
 */
@RunWith(Parameterized.class)
public class FetchResultSetTest extends HeadlessTest {

    private static final String TABLE_PREFIX = "TEST";
    protected String tableName;

    private Database db;

    public FetchResultSetTest(Database db) {
        this.db = db;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        List<Object[]> parameters = new ArrayList<Object[]>();

        for (Database db : Database.values()) {
            parameters.add(new Object[] { db });
        }

        return parameters;
    }

    @Before
    public void createTable() throws ClassNotFoundException {
        tableName = TABLE_PREFIX + Calendar.getInstance().getTimeInMillis();

        try {
            DatabaseHelper.executeUpdate(db, "CREATE TABLE " + tableName + "( "
            		+ "ID INTEGER PRIMARY KEY, "
                    + "CHAR_FIELD CHAR(25), "
                    + "INT_FIELD INTEGER, "
                    + "VARCHAR_FIELD VARCHAR(200) "
                    + ")");
        } catch (SQLException e) {
            throw new IllegalStateException(msg("Unable to setup database"), e);
        }
        String query = "INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (%d, '%s', %d, '%s')";
        try {
            DatabaseHelper.executeUpdate(
            		db, 
            		String.format(query, 1, "CHAR-content", 1234, "VARCHAR-content"),
                    String.format(query, 2, "CHAR-content", 1234, "VARCHAR-content"),
                    String.format(query, 3, "CHAR-content", 1234, "VARCHAR-content"),
                    String.format(query, 4, "CHAR-content", 1234, "VARCHAR-content")
            );
        } catch (SQLException e) {
            throw new IllegalStateException(msg("Unable to setup data"), e);
        }
    }

    /**
     * Test basic behavior of sql:fetch-resultset.
     * <p>
     * Expected: resultset contains 4 rows, values in rows match expected
     * values.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultSet() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");

        LogoList rows = (LogoList) workspace.report("sql:fetch-resultset");
        assertEquals(msg("Unexpected resultset size"), 4, rows.size());
        for (int i = 1; i <= 4; i++) {
            LogoList row = (LogoList) rows.get(i - 1);
            assertEquals(Double.valueOf(i), row.get(0));
            assertEquals(db.charValue("CHAR-content", 25), row.get(1));
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
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultSet_emptyResult() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " WHERE 1 = 0\"");

        LogoList rows = (LogoList) workspace.report("sql:fetch-resultset");
        assertEquals(msg("Expected empty list for empty resultset"), 0, rows.size());
    }

    /**
     * Test if a connection made with sql:connect is not autodisconnected after
     * fetching the resultset.
     * <p>
     * Expected: connection remains open after sql:fetch-resultset.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultset_connect_noAutodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");

        workspace.report("sql:fetch-resultset");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after fetch-resultset"), isConnected);
    }

    /**
     * Test if a connection made from connectionpool is not autodisconnected
     * after fetching the resultset when autodisconnect is disabled.
     * <p>
     * Expected: connection remains open after sql:fetch-resultset.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultset_connectionPool_noAutodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getPoolConfigurationCommand(false));
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");

        workspace.report("sql:fetch-resultset");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after fetch-resultset"), isConnected);
    }

    /**
     * Test if a connection made from connectionpool is autodisconnected after
     * fetching the resultset when autodisconnect is enabled.
     * <p>
     * Expected: connection is closed after sql:fetch-resultset.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultset_connectionPool_autodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getPoolConfigurationCommand());
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after select"), isConnected);

        workspace.report("sql:fetch-resultset");

        isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertFalse(msg("Should be auto-disconnected after fetch-resultset"), isConnected);
    }

    /**
     * Test if sql:fetch-resultset returns an empty logolist if called without a
     * connection.
     * <p>
     * Expected: empty list of type LogoList
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultSet_noConnection() throws Exception {
        workspace.open("init-sql.nlogo");

        Object resultset = workspace.report("sql:fetch-resultset");

        assertTrue(msg("Expected resultset of type LogoList"), resultset instanceof LogoList);
        assertEquals(msg("Expected emptylist as a result"), Collections.EMPTY_LIST, resultset);
    }

    /**
     * Test if sql:fetch-resultset returns an empty logolist if called with a
     * connection using sql:connect, but without a statement.
     * <p>
     * Expected: empty list of type LogoList
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultSet_connect_noStatement() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        Object resultset = workspace.report("sql:fetch-resultset");

        assertTrue(msg("Expected resultset of type LogoList"), resultset instanceof LogoList);
        assertEquals(msg("Expected emptylist as a result"), Collections.EMPTY_LIST, resultset);
    }

    /**
     * Test if sql:fetch-resultset returns an empty logolist if called with the
     * connectionpool, but without a statement.
     * <p>
     * Expected: empty list of type LogoList
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultSet_connectionpool_noStatement() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getPoolConfigurationCommand());

        Object resultset = workspace.report("sql:fetch-resultset");

        assertTrue(msg("Expected resultset of type LogoList"), resultset instanceof LogoList);
        assertEquals(msg("Expected emptylist as a result"), Collections.EMPTY_LIST, resultset);
    }

    /**
     * Test if sql:fetch-resultset returns an empty logolist if called after an
     * update statement.
     * <p>
     * Expected: empty list of type LogoList
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultSet_updateQuery() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());
        workspace.command("sql:exec-direct \"UPDATE " + tableName + "  SET INT_FIELD = 5\"");

        Object resultset = workspace.report("sql:fetch-resultset");

        assertTrue(msg("Expected resultset of type LogoList"), resultset instanceof LogoList);
        assertEquals(msg("Expected emptylist as a result"), Collections.EMPTY_LIST, resultset);
    }

    /**
     * Test if sql:fetch-resultset returns an empty list if called after a
     * single sql:fetch-row, even if more rows are available.
     * <p>
     * Expected: if sql:fetch-row has been used, then sql:fetch-resultset should
     * return an empty list.
     * </p>
     * <p>
     * Rationale: use either fetch-row, or fetch-resultset, do not combine these
     * reporters for the same resultset.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultSet_afterFetchRow() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
        workspace.report("sql:fetch-row");

        LogoList rows = (LogoList) workspace.report("sql:fetch-resultset");

        assertEquals(msg("Expected emptylist as a result"), Collections.EMPTY_LIST, rows);
    }

    /**
     * Test if calling sql:fetch-resultset for a second time returns an empty
     * list.
     * <p>
     * Expected: empty list returned on second call to sql:fetch-resultset.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFetchResultSet_secondCall() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " ORDER BY ID\"");
        workspace.report("sql:fetch-resultset");

        LogoList rows = (LogoList) workspace.report("sql:fetch-resultset");

        assertEquals(msg("Expected emptylist as a result"), Collections.EMPTY_LIST, rows);
    }

    // TODO cover more datatypes

    @After
    public void dropTable() {
        try {
            DatabaseHelper.executeUpdate(db, "DROP TABLE " + tableName);
        } catch (SQLException e) {
            throw new IllegalStateException(msg("Unable to drop table " + tableName), e);
        }
    }

    private String msg(String message) {
        return message + " (" + db.name() + ")";
    }

}
