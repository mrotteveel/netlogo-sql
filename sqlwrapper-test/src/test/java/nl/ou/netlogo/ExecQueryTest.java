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
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.nlogo.api.LogoList;
import org.nlogo.nvm.EngineException;

import nl.ou.netlogo.testsupport.Database;
import nl.ou.netlogo.testsupport.DatabaseHelper;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for the sql:exec-query command for all {@link Database} values.
 * 
 * @author Mark Rotteveel
 */
@RunWith(Parameterized.class)
public class ExecQueryTest extends HeadlessTest {

    private static final String TABLE_PREFIX = "TEST";
    protected String tableName;

    private Database db;

    public ExecQueryTest(Database db) {
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
                    String.format(query, 2, "CHAR-2", 3456, "VARCHAR-2")
            );
        } catch (SQLException e) {
            throw new IllegalStateException(msg("Unable to setup data"), e);
        }
    }

    /**
     * Test if the sql:exec-query works when no parameters are passed (ie: an
     * emptylist is passed).
     * <p>
     * Expected: Query works, resultset is available and contains 2 rows.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testSelect_noParameter() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-query \"SELECT * FROM " + tableName + " ORDER BY ID\" []");

        boolean hasResultSet = (Boolean) workspace.report("sql:resultset-available?");
        assertTrue(msg("Expect exec-query with select and no parameters to return a resultset"), hasResultSet);

        LogoList list = (LogoList) workspace.report("sql:fetch-resultset");
        assertEquals(msg("Unexpected resultset size"), 2, list.size());
    }

    /**
     * Test if the sql:exec-query works when a single parameter is passed into
     * the query.
     * <p>
     * Expected: Query works, result is available and contains 1 row, row
     * contains expected data.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testSelect_withParameter() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-query \"SELECT * FROM " + tableName + " WHERE ID = ?\" [2]");

        boolean hasResultSet = (Boolean) workspace.report("sql:resultset-available?");
        assertTrue(msg("Expect exec-query with select and no parameters to return a resultset"), hasResultSet);

        LogoList list = (LogoList) workspace.report("sql:fetch-resultset");
        assertEquals(msg("Unexpected resultset size"), 1, list.size());
        
        List<Object> expectedRow = Arrays.<Object> asList(Double.valueOf(2), db.charValue("CHAR-2", 25), Double.valueOf(3456),
                "VARCHAR-2");
        assertEquals(msg("Returned row had unexpected values"), expectedRow, list.get(0));
    }

    /**
     * Test if the connection created using sql:connect is not autodisconnected
     * after query execution.
     * <p>
     * Expected: connection remains open after sql:exec-query.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testSelect_connect_noAutodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-query \"SELECT * FROM " + tableName + " WHERE ID = ?\" [2]");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after select"), isConnected);
    }

    /**
     * Test if the connection created using the connection is not
     * autodisconnected after query execution when autodisconnect is disabled.
     * <p>
     * Expected: connection remains open after sql:exec-query.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testSelect_connectionPool_noAutodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getPoolConfigurationCommand(false));

        workspace.command("sql:exec-query \"SELECT * FROM " + tableName + " WHERE ID = ?\" [2]");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after select"), isConnected);
    }

    /**
     * Test if the connection created using the connection is not
     * autodisconnected after query execution when autodisconnect is enabled.
     * <p>
     * Expected: connection remains open after sql:exec-query.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testSelect_connectionPool_autodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getPoolConfigurationCommand());

        workspace.command("sql:exec-query \"SELECT * FROM " + tableName + " WHERE ID = ?\" [2]");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after select"), isConnected);
    }

    /**
     * Test if using invalid syntax in query throws an exception.
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testSyntaxError() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-query \"SLECT * FROM " + tableName + "\" []");
    }

    /**
     * Test if passing more parameters than expected throws an exception.
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testTooManyParameters() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-query \"SELECT * FROM " + tableName + " WHERE ID = ?\" [2 5]");
    }

    /**
     * Test if passing less parameters than expected throws an exception.
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testTooFewParameters() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-query \"SELECT * FROM " + tableName + " WHERE ID = ? OR ID = ?\" [2]");
    }

    /**
     * Test if passing no parameters when one is expected throws an exception.
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testNoParametersWhenExpected() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-query \"SELECT * FROM " + tableName + " WHERE ID = ?\" []");
    }

    /**
     * Test if passing a parameter when none is expected throws an exception.
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testParameterWhenNoneExpected() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-query \"SELECT * FROM " + tableName + "\" [2]");
    }

    /**
     * Test if passing a DELETE statement to sql:exec-query throws an exception.
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testExecQuery_delete() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-query \"DELETE FROM " + tableName + "\" []");
    }

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
