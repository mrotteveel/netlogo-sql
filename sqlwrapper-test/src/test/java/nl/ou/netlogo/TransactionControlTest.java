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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.nlogo.api.LogoList;
import org.nlogo.nvm.EngineException;

import nl.ou.netlogo.testsupport.DatabaseHelper;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for transaction control statements: sql:start-transaction,
 * sql:commit-transaction, sql:rollback-transaction.
 * 
 * @author Mark
 */
public class TransactionControlTest extends HeadlessTest {

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
                    + ") engine = InnoDB");
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to setup database", e);
        }
    }

    @Before
    public void setupData() {
        String query = "INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (%d, '%s', %d, '%s')";
        try {
            DatabaseHelper.executeUpdate(String.format(query, 1, "CHAR-content", 1234, "VARCHAR-content"));
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to setup data", e);
        }
    }

    /**
     * Test for sql:start-transaction (without any further commands).
     * <p>
     * Expected: autocommit disabled after execution of sql:start-transaction
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_StartTransaction_isolated() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());
        boolean isAutoCommit = (Boolean) workspace.report("sql:autocommit-enabled?");
        assertTrue("AutoCommit should be true before sql:start-transaction", isAutoCommit);

        workspace.command("sql:start-transaction");

        isAutoCommit = (Boolean) workspace.report("sql:autocommit-enabled?");
        assertFalse("AutoCommit should be false after sql:start-transaction", isAutoCommit);
    }

    /**
     * Test for transactional control from start to commit.
     * <p>
     * Expected: inserted row should be visible for other connection after
     * commit.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_CommitTransaction() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-update \"INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (?, ?, ?, ?)\"" +
        		" [2 \"text\" 567 \"text\"]");
        workspace.command("sql:commit-transaction");

        List<String> row = DatabaseHelper.executeSingletonQuery("SELECT COUNT(*) FROM " + tableName);
        assertEquals("Unexpected rowcount", "2", row.get(0));
    }

    /**
     * Test for transaction control from start to rollback.
     * <p>
     * Expected: inserted row should not be visible for other connection after
     * commit.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_RollbackTransaction() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-update \"INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (?, ?, ?, ?)\"" +
        		" [2 \"text\" 567 \"text\"]");
        workspace.command("sql:rollback-transaction");

        List<String> row = DatabaseHelper.executeSingletonQuery("SELECT COUNT(*) FROM " + tableName);
        assertEquals("Unexpected rowcount", "1", row.get(0));
    }

    /**
     * Test for visibility of transaction to other connection
     * <p>
     * Expected: changes to row not visible to other connection before commit,
     * visible after commit.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_TransactionVisibility_external() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-update \"UPDATE " + tableName + " SET CHAR_FIELD = ? WHERE ID = ?\" [\"text\" 1]");
        List<String> row = DatabaseHelper.executeSingletonQuery("SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = 1");
        assertEquals("Unexpected field value before commit", "CHAR-content", row.get(0));
        workspace.command("sql:commit-transaction");

        row = DatabaseHelper.executeSingletonQuery("SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = 1");
        assertEquals("Unexpected field value after commit", "text", row.get(0));
    }

    /**
     * Test for visibility of transaction to own connection.
     * <p>
     * Expected: changes to row visible to own connection before rollback,
     * changes reverted for own connection after rollback.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_TransactionVisibility_internal() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-update \"UPDATE " + tableName + " SET CHAR_FIELD = ? WHERE ID = ?\" [\"text\" 1]");
        workspace.command("sql:exec-query \"SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = ?\" [1]");
        LogoList row = (LogoList) workspace.report("sql:fetch-row");
        assertEquals("Unexpected field value before commit", "text", row.get(0));
        workspace.command("sql:rollback-transaction");

        workspace.command("sql:exec-query \"SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = ?\" [1]");
        row = (LogoList) workspace.report("sql:fetch-row");
        assertEquals("Unexpected field value after commit", "CHAR-content", row.get(0));
    }

    /**
     * Test if start-transaction can be left out when using sql:autocommit-off.
     * <p>
     * Expected: rollback-transaction will work if using sql:autocommit-off
     * instead of sql:start-transaction, changes not visible after rollback.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_NoStartTransaction_autocommitOff() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());

        workspace.command("sql:autocommit-off");
        workspace.command("sql:exec-update \"INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (?, ?, ?, ?)\"" +
        		" [2 \"text\" 567 \"text\"]");
        workspace.command("sql:rollback-transaction");

        List<String> row = DatabaseHelper.executeSingletonQuery("SELECT COUNT(*) FROM " + tableName);
        assertEquals("Unexpected rowcount", "1", row.get(0));
    }

    /**
     * Test if using commit-transaction throws an exception if autocommit is
     * enabled.
     * <p>
     * Expected: EngineException when calling sql:commit-transaction while in
     * autocommit mode.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    // TODO Use advanced JUnit features for checking exception message
    @Test(expected = EngineException.class)
    public void test_NoStartTransaction_commit_autocommitOn() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());

        workspace.command("sql:commit-transaction");
    }

    /**
     * Test if using rollback-transaction throws an exception if autocommit is
     * enabled.
     * <p>
     * Expected: EngineException when calling sql:rollback-transaction while in
     * autocommit mode.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    // TODO Use advanced JUnit features for checking exception message
    @Test(expected = EngineException.class)
    public void test_NoStartTransaction_rollback_autocommitOn() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());

        workspace.command("sql:rollback-transaction");
    }

    /**
     * Test if sql:fetch-row does not disconnect while a transaction is active
     * and autodisconnect is enabled.
     * <p>
     * Expected: No autodisconnect from fetch-row during a transaction.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_Autodisconnect_inTransaction_fetchRow() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " LIMIT 1\"");

        workspace.report("sql:fetch-row");

        assertTrue("Last fetch-row should not autodisconnect while in transaction", (Boolean) workspace.report("sql:debug-is-connected?"));
    }

    /**
     * Test if sql:fetch-resultset does not disconnect while a transaction is
     * active and autodisconnect is enabled.
     * <p>
     * Expected: No autodisconnect from fetch-resultset during a transaction.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_Autodisconnect_inTransaction_fetchResultset() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " LIMIT 1\"");

        workspace.report("sql:fetch-resultset");

        assertTrue("Last fetch-resultset should not autodisconnect while in transaction", (Boolean) workspace.report("sql:debug-is-connected?"));
    }

    /**
     * Test if an update statement (DELETE in this test) does not disconnect
     * while a transaction is active and autodisconnect is enabled.
     * <p>
     * Expected: No autodisconnect after update statement (DELETE in this test)
     * during a transaction.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_Autodisconnect_inTransaction_update() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"DELETE FROM " + tableName + "\"");

        assertTrue("Update statements should not autodisconnect while in transaction", (Boolean) workspace.report("sql:debug-is-connected?"));
    }

    /**
     * Test if sql:commit-transaction does disconnect with autodisconnect is
     * enabled.
     * <p>
     * Expected: sql:commit-transaction performs autodisconnect.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_Autodisconnect_commit() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"DELETE FROM " + tableName + "\"");

        assertTrue("Update statements should not autodisconnect while in transaction", (Boolean) workspace.report("sql:debug-is-connected?"));
        workspace.command("sql:commit-transaction");
        assertFalse("sql:commit-transaction should autodisconnect", (Boolean) workspace.report("sql:debug-is-connected?"));
    }

    /**
     * Test if sql:rollback-transaction does disconnect with autodisconnect is
     * enabled.
     * <p>
     * Expected: sql:rollback-transaction performs autodisconnect.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_Autodisconnect_rollback() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"DELETE FROM " + tableName + "\"");

        assertTrue("Update statements should not autodisconnect while in transaction", (Boolean) workspace.report("sql:debug-is-connected?"));
        workspace.command("sql:rollback-transaction");
        assertFalse("sql:rollback-transaction should autodisconnect", (Boolean) workspace.report("sql:debug-is-connected?"));
    }

    /**
     * Test if sql:autocommit-on does disconnect with autodisconnect is enabled
     * and a transaction was active.
     * <p>
     * Expected: sql:autocommit-on performs autodisconnect.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void test_Autodisconnect_autocommitOn() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"DELETE FROM " + tableName + "\"");

        assertTrue("Update statements should not autodisconnect while in transaction", (Boolean) workspace.report("sql:debug-is-connected?"));
        workspace.command("sql:autocommit-on");
        assertFalse("sql:autocommit-on should autodisconnect", (Boolean) workspace.report("sql:debug-is-connected?"));
    }

    @After
    public void teardownData() {
        try {
            workspace.command("sql:rollback-transaction");
        } catch (Exception ex) {
            // ignore, just ensuring no transaction is running
        }
        try {
            DatabaseHelper.executeUpdate("TRUNCATE TABLE " + tableName);
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to delete data", e);
        }
    }

    @AfterClass
    public static void dropTable() {
        try {
            DatabaseHelper.executeUpdate(new String[] { "DROP TABLE " + tableName });
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to drop table " + tableName, e);
        }
    }

}
