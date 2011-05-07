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

import static nl.ou.netlogo.testsupport.DatabaseHelper.getMySQLPoolConfigurationCommand;
import static nl.ou.netlogo.testsupport.DatabaseHelper.getMySQLConnectCommand;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import nl.ou.netlogo.testsupport.HeadlessTest;

import org.junit.Test;
import org.nlogo.agent.AgentSet;
import org.nlogo.agent.Turtle;

/**
 * Tests for sql:is-connected? (and partially for sql:debug-is-connected?).
 * 
 * @author Mark Rotteveel
 */
public class IsConnectedTest extends HeadlessTest {
	
	/**
	 * Test if sql:is-connected? correctly reports on connection status after sql:connect.
	 * <p>
	 * Expected: sql:is-connected? returns true
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testIsConnected_whenConnected() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLConnectCommand());
		
		assertTrue("Expected true for sql:is-connected?", (Boolean)workspace.report("sql:is-connected?"));
	}
	
	/**
	 * Test if sql:is-connected? correctly reports on connection status without connection.
	 * <p>
	 * Expected: sql:is-connected? returns false
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testIsConnected_whenNotConnected() throws Exception {
		workspace.open("init-sql.nlogo");
		
		assertFalse("Expected false for sql:is-connected?", (Boolean)workspace.report("sql:is-connected?"));
	}
	
	/**
	 * Test if sql:is-connected? correctly reports on connection status when connectionpooling is enabled.
	 * <p>
	 * Expected: sql:debug-is-connected? returns false, sql:is-connected? returns true
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_isConnected() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLPoolConfigurationCommand());

		assertFalse("Expected debug-is-connected? to return false", (Boolean)workspace.report("sql:debug-is-connected?"));
		assertTrue("Expected is-connected? to return true with connectionpooling", (Boolean)workspace.report("sql:is-connected?"));
		assertFalse("Expected is-connected? to not establish a real conenction, debug-is-connected? should return false", 
				(Boolean)workspace.report("sql:debug-is-connected?"));
	}
	
	/**
	 * Test if sql:is-connected? correctly reports on connection status after explicit disconnect when connectionpooling is enabled.
	 * <p>
	 * Expected: sql:debug-is-connected? returns false, sql:is-connected? returns true
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_isConnected_afterDisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getMySQLPoolConfigurationCommand());

		// Establish actual connection
		workspace.command("sql:exec-direct \"SELECT 1\"");
		assertTrue("Expected debug-is-connected? to return true", (Boolean)workspace.report("sql:debug-is-connected?"));
		
		workspace.command("sql:disconnect");
		assertFalse("Expected debug-is-connected? to return false", (Boolean)workspace.report("sql:debug-is-connected?"));
		assertTrue("Expected is-connected? to return true", (Boolean)workspace.report("sql:is-connected?"));
	}
	
	/**
	 * Test if sql:is-connected? correctly reports on connection status of distinct agents.
	 * <p>
	 * Expected: connected=true for connected agent, and false for unconnected agent.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testIsConnected_distinctAgents() throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		Turtle turtle1 = workspace.world.createTurtle(breed);
		Turtle turtle2 = workspace.world.createTurtle(breed);
		
		workspace.evaluateCommands(getMySQLConnectCommand(), turtle2, true);
		
		assertFalse("Expected turtle1 to not be connected", (Boolean)workspace.evaluateReporter("sql:is-connected?", turtle1));
		assertTrue("Expected turtle2 to be connected", (Boolean)workspace.evaluateReporter("sql:is-connected?", turtle2));
	}
}
