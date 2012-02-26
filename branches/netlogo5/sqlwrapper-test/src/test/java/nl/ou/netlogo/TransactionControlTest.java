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
 * Tests for transaction control statements: sql:start-transaction,
 * sql:commit-transaction, sql:rollback-transaction for all {@link Database}
 * values.
 * 
 * @author Mark
 */
@RunWith(Parameterized.class)
public class TransactionControlTest extends HeadlessTest {

    private static final String TABLE_PREFIX = "TEST";
    protected String tableName;

    private Database db;

    public TransactionControlTest(Database db) {
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
                    + ")"
                    + (db.equals(Database.MYSQL) ? " engine = InnoDB" : ""));
        } catch (SQLException e) {
            throw new IllegalStateException(msg("Unable to setup database"), e);
        }
        String query = "INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (%d, '%s', %d, '%s')";
        try {
            DatabaseHelper.executeUpdate(db, String.format(query, 1, "CHAR-content", 1234, "VARCHAR-content"));
        } catch (SQLException e) {
            throw new IllegalStateException(msg("Unable to setup data"), e);
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
        workspace.command(db.getConnectCommand());
        boolean isAutoCommit = (Boolean) workspace.report("sql:autocommit-enabled?");
        assertTrue(msg("AutoCommit should be true before sql:start-transaction"), isAutoCommit);

        workspace.command("sql:start-transaction");

        isAutoCommit = (Boolean) workspace.report("sql:autocommit-enabled?");
        assertFalse(msg("AutoCommit should be false after sql:start-transaction"), isAutoCommit);
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
        workspace.command(db.getConnectCommand());

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-update \"INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (?, ?, ?, ?)\"" + 
        		" [2 \"text\" 567 \"text\"]");
        workspace.command("sql:commit-transaction");

        List<String> row = DatabaseHelper.executeSingletonQuery(db, "SELECT COUNT(*) FROM " + tableName);
        assertEquals(msg("Unexpected rowcount"), "2", row.get(0));
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
        workspace.command(db.getConnectCommand());

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-update \"INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (?, ?, ?, ?)\"" + 
        		" [2 \"text\" 567 \"text\"]");
        workspace.command("sql:rollback-transaction");

        List<String> row = DatabaseHelper.executeSingletonQuery(db, "SELECT COUNT(*) FROM " + tableName);
        assertEquals(msg("Unexpected rowcount"), "1", row.get(0));
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
        workspace.command(db.getConnectCommand());

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-update \"UPDATE " + tableName + " SET CHAR_FIELD = ? WHERE ID = ?\" [\"text\" 1]");
        List<String> row = DatabaseHelper.executeSingletonQuery(db, "SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = 1");
        assertEquals(msg("Unexpected field value before commit"), db.charValue("CHAR-content", 25), row.get(0));
        workspace.command("sql:commit-transaction");

        row = DatabaseHelper.executeSingletonQuery(db, "SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = 1");
        assertEquals(msg("Unexpected field value after commit"), db.charValue("text", 25), row.get(0));
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
        workspace.command(db.getConnectCommand());

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-update \"UPDATE " + tableName + " SET CHAR_FIELD = ? WHERE ID = ?\" [\"text\" 1]");
        workspace.command("sql:exec-query \"SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = ?\" [1]");
        LogoList row = (LogoList) workspace.report("sql:fetch-row");
        assertEquals(msg("Unexpected field value before commit"), db.charValue("text", 25), row.get(0));
        workspace.command("sql:rollback-transaction");

        workspace.command("sql:exec-query \"SELECT CHAR_FIELD FROM " + tableName + " WHERE ID = ?\" [1]");
        row = (LogoList) workspace.report("sql:fetch-row");
        assertEquals(msg("Unexpected field value after commit"), db.charValue("CHAR-content", 25), row.get(0));
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
        workspace.command(db.getConnectCommand());

        workspace.command("sql:autocommit-off");
        workspace.command("sql:exec-update \"INSERT INTO " + tableName + "(ID, CHAR_FIELD, INT_FIELD, VARCHAR_FIELD) VALUES (?, ?, ?, ?)\"" + 
        		" [2 \"text\" 567 \"text\"]");
        workspace.command("sql:rollback-transaction");

        List<String> row = DatabaseHelper.executeSingletonQuery(db, "SELECT COUNT(*) FROM " + tableName);
        assertEquals(msg("Unexpected rowcount"), "1", row.get(0));
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
        workspace.command(db.getConnectCommand());

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
        workspace.command(db.getConnectCommand());

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
        workspace.command(db.getPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " LIMIT 1\"");

        workspace.report("sql:fetch-row");

        assertTrue(msg("Last fetch-row should not autodisconnect while in transaction"), (Boolean) workspace.report("sql:debug-is-connected?"));
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
        workspace.command(db.getPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"SELECT * FROM " + tableName + " LIMIT 1\"");

        workspace.report("sql:fetch-resultset");

        assertTrue(msg("Last fetch-resultset should not autodisconnect while in transaction"), (Boolean) workspace.report("sql:debug-is-connected?"));
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
        workspace.command(db.getPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"DELETE FROM " + tableName + "\"");

        assertTrue(msg("Update statements should not autodisconnect while in transaction"), (Boolean) workspace.report("sql:debug-is-connected?"));
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
        workspace.command(db.getPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"DELETE FROM " + tableName + "\"");

        assertTrue(msg("Update statements should not autodisconnect while in transaction"), (Boolean) workspace.report("sql:debug-is-connected?"));
        workspace.command("sql:commit-transaction");
        assertFalse(msg("sql:commit-transaction should autodisconnect"), (Boolean) workspace.report("sql:debug-is-connected?"));
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
        workspace.command(db.getPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"DELETE FROM " + tableName + "\"");

        assertTrue(msg("Update statements should not autodisconnect while in transaction"), (Boolean) workspace.report("sql:debug-is-connected?"));
        workspace.command("sql:rollback-transaction");
        assertFalse(msg("sql:rollback-transaction should autodisconnect"), (Boolean) workspace.report("sql:debug-is-connected?"));
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
        workspace.command(db.getPoolConfigurationCommand(true));

        workspace.command("sql:start-transaction");
        workspace.command("sql:exec-direct \"DELETE FROM " + tableName + "\"");

        assertTrue(msg("Update statements should not autodisconnect while in transaction"), (Boolean) workspace.report("sql:debug-is-connected?"));
        workspace.command("sql:autocommit-on");
        assertFalse(msg("sql:autocommit-on should autodisconnect"), (Boolean) workspace.report("sql:debug-is-connected?"));
    }

    @After
    public void dropTable() {
        try {
            workspace.command("sql:rollback-transaction");
        } catch (Exception ex) {
            // ignore, just ensuring no transaction is running
        }
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
