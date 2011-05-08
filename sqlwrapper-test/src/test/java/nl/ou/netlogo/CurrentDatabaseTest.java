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

import static nl.ou.netlogo.testsupport.DatabaseHelper.getGenericConnectCommand;
import static nl.ou.netlogo.testsupport.DatabaseHelper.getMySQLPoolConfigurationCommand;
import static nl.ou.netlogo.testsupport.DatabaseHelper.getMySQLConnectCommand;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import nl.ou.netlogo.testsupport.ConnectionInformation;
import nl.ou.netlogo.testsupport.HeadlessTest;

import org.junit.Test;
import org.nlogo.nvm.EngineException;

/**
 * Tests for sql:current-database.
 * 
 * @author Mark Rotteveel
 */
public class CurrentDatabaseTest extends HeadlessTest {

    /**
     * Test if sql:current-database will return schema name used in the connect
     * command.
     * <p>
     * Expected: returned schema name is equal to schema name used in connect.
     * </p>
     * <p>
     * Assumption: schema name used in test has same casing as schema name on
     * database.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testCurrentDatabase_afterConnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getMySQLConnectCommand());

        String currentDB = (String) workspace.report("sql:current-database");

        assertEquals(ConnectionInformation.getInstance().getSchema(), currentDB);
    }

    // TODO Use advanced junit features to check exception message

    /**
     * Test if sql:current-database throws an error if no connection is
     * established (and no pooling).
     * <p>
     * Expected: throws exception
     * </p>
     * 
     * @throws Exception
     */
    @Test(expected = EngineException.class)
    public void testCurrentDatabase_noConnection() throws Exception {
        workspace.open("init-sql.nlogo");

        workspace.report("sql:current-database");
    }

    /**
     * Test if sql:current-database does not autodisconnect when connection
     * created using sql:connect.
     * <p>
     * Expected: connection is not auto-disconnected after sql:current-database
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testCurrentDatabase_connect_noAutodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getMySQLConnectCommand());

        workspace.report("sql:current-database");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue("No autodisconnect expected", isConnected);
    }

    /**
     * Test if sql:current-database will return schema name used in the
     * defaultconnection configuration.
     * <p>
     * Expected: returned schema name is equal to schema name used in
     * defaultconnection configuration.
     * </p>
     * <p>
     * Assumption: schema name used in test has same casing as schema name on
     * database.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testCurrentDatabase_connectionPool() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getMySQLPoolConfigurationCommand());

        String currentDB = (String) workspace.report("sql:current-database");

        assertEquals(ConnectionInformation.getInstance().getSchema(), currentDB);
    }

    /**
     * Test if sql:current-database does not autodisconnect when connection
     * created using connectionpool with autodisconnect disabled.
     * <p>
     * Expected: connection is not auto-disconnected after sql:current-database
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testCurrentDatabase_connectionPool_noAutodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getMySQLPoolConfigurationCommand(false));

        workspace.report("sql:current-database");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertTrue("No autodisconnect expected", isConnected);
    }

    /**
     * Test if sql:current-database does autodisconnect when connection created
     * from connectionpool when autodisconnect is enabled for the connection
     * pool.
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testCurrentDatabase_autodisconnect_connectionPool_enabledForPool() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getMySQLPoolConfigurationCommand(true));
        workspace.command("sql:configure \"defaultconnection\" [[\"autodisconnect\" \"on\"]]");

        workspace.report("sql:current-database");

        boolean isConnected = (Boolean) workspace.report("sql:debug-is-connected?");
        assertFalse("Autodisconnect was expected", isConnected);
    }

    /**
     * Test if sql:current-database returns <code>default</code> on a brand
     * generic connection.
     * <p>
     * Expected: returns <code>default</code> for a brand generic connection.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testCurrentDatabase_generic() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getGenericConnectCommand());

        String currentDB = (String) workspace.report("sql:current-database");

        assertEquals("Unexpected value for sql:current-database on generic connection", "default", currentDB);
    }

}
