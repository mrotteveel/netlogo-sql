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
import static nl.ou.netlogo.testsupport.DatabaseHelper.getDefaultConnectCommand;
import static nl.ou.netlogo.testsupport.DatabaseHelper.getDefaultPoolConfigurationCommand;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.nlogo.nvm.EngineException;

import nl.ou.netlogo.testsupport.Database;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for the sql:find-database reporter.
 * 
 * @author Mark Rotteveel
 */
public class FindDatabaseTest extends HeadlessTest {

    /**
     * Test if sql:find-database correctly reports on the existence of a known
     * schema (on a connection created with sql:connect).
     * <p>
     * Expected: sql:find-database returns true.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFindDatabase_exists() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());
        Boolean dbFound = (Boolean) workspace.report("sql:find-database \"" + Database.MYSQL.getSchema() + "\"");
        assertTrue("Existing database schema should be found", dbFound);
    }

    /**
     * Test if sql:find-database works on a pooled connection.
     * <p>
     * Expected: sql:find-database returns true.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFindDatabase_connectionpool() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultPoolConfigurationCommand());
        Boolean dbFound = (Boolean) workspace.report("sql:find-database \"" + Database.MYSQL.getSchema() + "\"");
        assertTrue("Existing database schema should be found", dbFound);
    }

    /**
     * Test if sql:find-database performs an autodisconnect on a pooled
     * connection with autodisconnect enabled.
     * <p>
     * Expected: sql:find-database performs an autodisconnect.
     * </p>
     * 
     * @throws Exception
     */
    @Test
    public void testFindDatabase_connectionPool_autodisconnect() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultPoolConfigurationCommand(true));

        workspace.report("sql:find-database \"" + Database.MYSQL.getSchema() + "\"");

        assertFalse("Expected autodisconnect after sql:find-database",
                (Boolean) workspace.report("sql:debug-is-connected?"));
    }

    /**
     * Test if sql:find-database correctly reports on the existence of a schema
     * that does not exist.
     * <p>
     * Expected: sql:find-database returns false.
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFindDatabase_notExists() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getDefaultConnectCommand());
        Boolean dbFound = (Boolean) workspace.report("sql:find-database \"DOESNOTEXIST\"");
        assertFalse("Non-existent database schema should not be found", dbFound);
    }

    /**
     * Test if sql:find-database throws an exception if there is no database
     * connection.
     * <p>
     * Expected: throws exception
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test(expected = EngineException.class)
    public void testFindDatabase_noConnection() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.report("sql:find-database \"" + Database.MYSQL.getSchema() + "\"");
    }

    /**
     * Test if sql:find-database returns <code>false</code> if used on a brand
     * generic connection for a schema known to exist
     * <p>
     * Expected: sql:find-database on generic database returns
     * <code>false</code> always
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFindDatabase_generic_existingSchema() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getGenericConnectCommand());
        Boolean dbFound = (Boolean) workspace.report("sql:find-database \"" + Database.MYSQL.getSchema() + "\"");
        assertFalse("sql:find-database on a generic connection should return false", dbFound);
    }

    /**
     * Test if sql:find-database return false if used on a brand generic
     * connection for a schema that is know to not exist.
     * <p>
     * Expected: sql:find-database on generic database returns
     * <code>false</code> always
     * </p>
     * 
     * @throws Exception
     *             For any exceptions during testing
     */
    @Test
    public void testFindDatabase_generic_notExists() throws Exception {
        workspace.open("init-sql.nlogo");
        workspace.command(getGenericConnectCommand());

        Boolean dbFound = (Boolean) workspace.report("sql:find-database \"DOESNOTEXIST\"");

        assertFalse("sql:find-database on a generic connection should return false", dbFound);
    }
}
