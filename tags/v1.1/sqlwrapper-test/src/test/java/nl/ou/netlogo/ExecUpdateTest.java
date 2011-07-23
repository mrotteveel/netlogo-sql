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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.nlogo.nvm.EngineException;

import nl.ou.netlogo.testsupport.Database;
import nl.ou.netlogo.testsupport.DatabaseHelper;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for sql:exec-update for all {@link Database} values.
 * <p>
 * Note that most update related tests are only tested with UPDATE statements,
 * as also testing INSERT and DELETE can be considered testing the underlying
 * JDBC driver.
 * </p>
 * 
 * @author Mark Rotteveel
 */
@RunWith(Parameterized.class)
public class ExecUpdateTest extends HeadlessTest {

    private static final String TABLE_PREFIX = "TEST";
    protected String tableName;

    private Database db;

    public ExecUpdateTest(Database db) {
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
     * Test for sql:exec-update without parameters.
     * <p>
     * Expected: query works, no resultset available, all rows are deleted from
     * test table
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testDelete_noParameter() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-update \"DELETE FROM " + tableName + "\" []");

        boolean hasResultSet = (Boolean) workspace.report("sql:resultset-available?");
        assertFalse(msg("Expect exec-update to have no resultset available"), hasResultSet);

        double affectedRows = (Double) workspace.report("sql:get-rowcount");
        assertEquals(msg("Expected all rows (2) to be changed"), 2, (int) affectedRows);

        List<String> row = DatabaseHelper.executeSingletonQuery(db, "SELECT COUNT(*) FROM " + tableName);
        assertEquals(msg("Expected no rows in " + tableName), "0", row.get(0));
    }

    /**
     * Test if single parameter substitution works in an UPDATE without a WHERE
     * condition.
     * <p>
     * Expected: query works, no resultset available, all rows in test table are
     * changed.
     * </p>
     * <p>
     * NOTE: Together with {@link #testUpdate_withWhereCondition()} this can be
     * considered testing the JDBC driver
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testUpdate_noWhereCondition() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-update \"UPDATE " + tableName + " SET CHAR_FIELD = ?\" [\"updated\"]");

        boolean hasResultSet = (Boolean) workspace.report("sql:resultset-available?");
        assertFalse(msg("Expect exec-update to have no resultset available"), hasResultSet);

        double affectedRows = (Double) workspace.report("sql:get-rowcount");
        assertEquals(msg("Expected all rows (2) to be changed"), 2, (int) affectedRows);

        List<String> row = DatabaseHelper.executeSingletonQuery(db, "SELECT COUNT(*) FROM " + tableName + " WHERE CHAR_FIELD = 'updated'");
        assertEquals(msg("Expected two rows to have value of CHAR_FIELD changed to 'updated'"), "2", row.get(0));
    }

    /**
     * Test if multiple parameter substitution works in an UPDATE with a WHERE
     * condition.
     * <p>
     * Expected: query works, no resultset available, only specified row in test
     * table is changed.
     * </p>
     * <p>
     * NOTE: Together with {@link #testUpdate_noWhereCondition()} this can be
     * considered testing the JDBC driver
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testUpdate_withWhereCondition() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-update \"UPDATE " + tableName + " SET CHAR_FIELD = ? WHERE ID = ?\" [\"updated\" 2]");

        boolean hasResultSet = (Boolean) workspace.report("sql:resultset-available?");
        assertFalse(msg("Expect exec-update to have no resultset available"), hasResultSet);

        double affectedRows = (Double) workspace.report("sql:get-rowcount");
        assertEquals(msg("Expected one (1) to be changed"), 1, (int) affectedRows);

        List<String> row = DatabaseHelper.executeSingletonQuery(db, "SELECT COUNT(*) FROM " + tableName + " WHERE CHAR_FIELD = 'updated'");
        assertEquals(msg("Expected one (1) row to have value of CHAR_FIELD changed to 'updated'"), "1", row.get(0));

        row = DatabaseHelper.executeSingletonQuery(db, "SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = 1");
        assertEquals(msg("Expected CHAR_FIELD OF ID = 1 to be 'CHAR-content'"), db.charValue("CHAR-content", 25), row.get(0));

        row = DatabaseHelper.executeSingletonQuery(db, "SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = 2");
        assertEquals(msg("Expected CHAR_FIELD OF ID = 2 to be 'updated'"), db.charValue("updated", 25), row.get(0));
    }

    /**
     * Test if the connection allocated with sql:connect is not autodisconnected
     * after sql:exec-update.
     * <p>
     * Expected: connection remains open after sql:exec-update.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testUpdate_connect_noAutodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-update \"UPDATE " + tableName + " SET CHAR_FIELD = ? WHERE ID = ?\" [\"updated\" 2]");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after update"), isConnected);
    }

    /**
     * Test if the connection allocated from connectionpool is not
     * autodisconnected after sql:exec-update when autodisconnect is disabled.
     * <p>
     * Expected: connection remains open after sql:exec-update.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testUpdate_connectionPool_noAutodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getPoolConfigurationCommand(false));

        workspace.command("sql:exec-update \"UPDATE " + tableName + " SET CHAR_FIELD = ? WHERE ID = ?\" [\"updated\" 2]");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after update"), isConnected);
    }

    /**
     * Test if the connection allocated from connectionpool is autodisconnected
     * after sql:exec-update when autodisconnect is enabled.
     * <p>
     * Expected: connection is closed after sql:exec-update, no resultset
     * available.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testUpdate_connectionPool_autodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getPoolConfigurationCommand());

        workspace.command("sql:exec-update \"UPDATE " + tableName + " SET CHAR_FIELD = ? WHERE ID = ?\" [\"updated\" 2]");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertFalse(msg("Should have been auto-disconnected after update"), isConnected);

        boolean hasResultSet = (Boolean) workspace.report("sql:resultset-available?");
        assertFalse(msg("Expect exec-update to have no resultset available"), hasResultSet);
    }

    /**
     * Test if an insert works with sql:exec-update.
     * <p>
     * Expected: INSERT statement works and test table includes new data.
     * </p>
     * <p>
     * NOTE: This test is essentially checking if the JDBC driver works.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testInsert() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-update \"INSERT INTO " + tableName + " (ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (?, ?, ?, ?)\" [3 \"A String\" 513 \"Another String\"]");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after update"), isConnected);

        double affectedRows = (Double) workspace.report("sql:get-rowcount");
        assertEquals(msg("Expected one (1) to be changed"), 1, (int) affectedRows);

        List<String> row = DatabaseHelper.executeSingletonQuery(db, "SELECT COUNT(*) FROM " + tableName);
        assertEquals(msg("Expected three (3) rows in table"), "3", row.get(0));

        row = DatabaseHelper.executeSingletonQuery(db, "SELECT CHAR_FIELD, INT_FIELD, VARCHAR_FIELD FROM " + tableName + " WHERE ID = 3");
        List<String> expectedRow = Arrays.asList(db.charValue("A String", 25), "513", "Another String");
        assertEquals(msg("Row with ID=3 has unexpected content"), expectedRow, row);
    }

    /**
     * Test if a delete works with sql:exec-update.
     * <p>
     * Expected: DELETE statement works and identified row is deleted from test
     * table.
     * </p>
     * <p>
     * NOTE: This test is essentially checking if the JDBC driver works.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testDelete() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-update \"DELETE FROM " + tableName + " WHERE VARCHAR_FIELD = ? AND INT_FIELD = ?\" [\"VARCHAR-2\" 3456]");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue(msg("Should stay connected after update"), isConnected);

        double affectedRows = (Double) workspace.report("sql:get-rowcount");
        assertEquals(msg("Expected one (1) to be changed"), 1, (int) affectedRows);

        List<String> row = DatabaseHelper.executeSingletonQuery(db, "SELECT COUNT(*) FROM " + tableName);
        assertEquals(msg("Expected one (1) row in table"), "1", row.get(0));

        row = DatabaseHelper.executeSingletonQuery(db, "SELECT ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD FROM " + tableName);
        List<String> expectedRow = Arrays.asList("1", db.charValue("CHAR-content", 25), "1234", "VARCHAR-content");
        assertEquals(msg("Remaining row has unexpected content"), expectedRow, row);
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

        workspace.command("sql:exec-update \"DLETE FROM " + tableName + "\" []");
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

        workspace.command("sql:exec-update \"DELETE FROM " + tableName + " WHERE ID = ?\" [2 5]");
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

        workspace.command("sql:exec-update \"DELETE FROM " + tableName + " WHERE ID = ? OR ID = ?\" [2]");
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

        workspace.command("sql:exec-update \"DELETE FROM " + tableName + " WHERE ID = ?\" []");
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

        workspace.command("sql:exec-update \"DELETE FROM " + tableName + "\" [2]");
    }

    /**
     * Test if applying a SELECT statement in sql:exec-update throws an
     * exception.
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testExecUpdate_select() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(db.getConnectCommand());

        workspace.command("sql:exec-update \"SELECT * FROM " + tableName + "\" []");
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
