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
import static nl.ou.netlogo.testsupport.DatabaseHelper.getMySQLConnectCommand;
import static nl.ou.netlogo.testsupport.DatabaseHelper.getMySQLPoolConfigurationCommand;
import static org.junit.Assert.assertEquals;
import nl.ou.netlogo.testsupport.ConnectionInformation;
import nl.ou.netlogo.testsupport.HeadlessTest;

import org.junit.Test;
import org.nlogo.api.LogoList;
import org.nlogo.nvm.EngineException;

/**
 * Tests for sql:use-database
 * 
 * @author Mark Rotteveel
 */
public class UseDatabaseTest extends HeadlessTest {

    /**
     * Test if sql:use-database is able to switch to an existing schema (too
     * which the user has access).
     * <p>
     * Expected: switch is successful.
     * </p>
     * <p>
     * Assumptions: configured database schema for the test is not
     * "information_schema", and test user has access to information_schema.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testUseDatabase_existingSchema() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getMySQLConnectCommand());
        workspace.command("sql:exec-direct \"SELECT DATABASE()\"");
        LogoList list = (LogoList) workspace.report("sql:fetch-row");
        assertEquals("Unexpected database name", ConnectionInformation.getInstance().getSchema(), list.get(0));

        workspace.command("sql:use-database \"information_schema\"");

        workspace.command("sql:exec-direct \"SELECT DATABASE()\"");
        list = (LogoList) workspace.report("sql:fetch-row");
        assertEquals("Unexpected database name", "information_schema", list.get(0));
    }

    // TODO use advanced junit features to check exception

    /**
     * Test if sql:use-database throws an exception if asked to switch to a
     * non-existent schema.
     * <p>
     * Expected: throws exception
     * </p>
     * <p>
     * Assumptions: schema "DOESNOTEXIST" does not exist on the test database
     * server.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testUseDatabase_nonExistentSchema() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getMySQLConnectCommand());

        workspace.command("sql:use-database \"DOESNOTEXIST\"");
    }

    /**
     * Test if sql:use-database throws an exception if used on a pooled
     * connection.
     * <p>
     * Expected: throws exception
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testUseDatabase_connectionPool() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getMySQLPoolConfigurationCommand());

        workspace.command("sql:use-database \"information_schema\"");
    }

    /**
     * Test if sql:use-database does not throw an exception when called on a
     * brand generic connection.
     * <p>
     * Expected: Does nothing
     * </p>
     * <p>
     * TODO Add additional validation if use-database is an actual no-op for
     * generic database connection.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testUseDatabase_generic() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getGenericConnectCommand());

        workspace.command("sql:use-database \"information_schema\"");
    }

}
