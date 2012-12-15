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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import nl.ou.netlogo.testsupport.HeadlessTest;

import org.junit.Test;
import org.nlogo.agent.AgentSet;
import org.nlogo.agent.Turtle;

/**
 * Tests for sql:disconnect.
 * 
 * @author Mark Rotteveel
 */
public class DisconnectTest extends HeadlessTest {
	
	/**
	 * Test if sql:disconnect closes the connection when a connection is established.
	 * <p>
	 * Expected: connection is closed after sql:disconnect.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testDisconnect_whenConnected() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultConnectCommand());
		assertTrue("Expected true for sql:is-connected?", (Boolean)workspace.report("sql:is-connected?"));
		
		workspace.command("sql:disconnect");
		
		assertFalse("Expected false for sql:is-connected?", (Boolean)workspace.report("sql:is-connected?"));
	}
	
	/**
	 * Test if sql:disconnect works (ie: does not throw an error) if called when no connection is established (for observer).
	 * <p>
	 * Expected: sql:disconnect does not throw an error when called when no connection is established.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testDisconnect_whenNotConnected() throws Exception {
		workspace.open("init-sql.nlogo");
		assertFalse("Expected false for sql:is-connected?", (Boolean)workspace.report("sql:is-connected?"));
		
		workspace.command("sql:disconnect");
		
		assertFalse("Expected false for sql:is-connected?", (Boolean)workspace.report("sql:is-connected?"));
	}
	
	/**
	 * Test if sql:disconnect only closes the connection of the current agent, and not for another agent (the observer in this test).
	 * <p>
	 * Expected: only connection of the agent is closed, connection of observer remains open.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testDisconnectAgent_whenObserverAndAgentConnected() throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		Turtle turtle = workspace.world.createTurtle(breed);
		assertNotNull("Unable to create turtle", turtle);
		
		workspace.command(getDefaultConnectCommand());
		workspace.evaluateCommands(workspace.defaultOwner(), getDefaultConnectCommand(), turtle, true);
		
		assertTrue("Expected true for sql:is-connected? of observer", (Boolean)workspace.report("sql:is-connected?"));
		assertTrue("Expected true for sql:is-connected? of agent", (Boolean)workspace.evaluateReporter(workspace.defaultOwner(), "sql:is-connected?", turtle));
		
		workspace.evaluateCommands(workspace.defaultOwner(), "sql:disconnect", turtle, true);
		
		assertTrue("Expected true for sql:is-connected? of observer", (Boolean)workspace.report("sql:is-connected?"));
		assertFalse("Expected false for sql:is-connected? of agent", (Boolean)workspace.evaluateReporter(workspace.defaultOwner(), "sql:is-connected?", turtle));
	}
	
	/**
	 * Test if disconnect command does not throw an error when called in isolation with connection pooling.
	 * <p>
	 * Expected: sql:disconnect does not throw an error, and no connection is open after the disconnect.
	 * </p>
	 *
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_isolatedDisconnect() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		
		assertFalse("Expected false for sql:debug-is-connected? of observer", (Boolean)workspace.report("sql:debug-is-connected?"));
		workspace.command("sql:disconnect");
		assertFalse("Expected false for sql:debug-is-connected? of observer", (Boolean)workspace.report("sql:debug-is-connected?"));
	}
}
