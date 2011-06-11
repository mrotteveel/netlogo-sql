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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.nlogo.agent.AgentSet;
import org.nlogo.agent.Turtle;

import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Tests for the autocommit status of a connection and changes to that status.
 * 
 * @author Mark Rotteveel
 */
public class AutoCommitEnabledTest extends HeadlessTest {
	
	/**
	 * Test default autocommit status of a newly created connection.
	 * <p>
	 * Expected: Connection has autocommit enabled.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testAutoCommitDefault() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultConnectCommand());
		
		boolean isAutoCommit = (Boolean)workspace.report("sql:autocommit-enabled?");
		assertTrue("AutoCommit should be true by default", isAutoCommit);
	}
	
	/**
	 * Test autocommit status after disabling autocommit using sql:autocommit-off.
	 * <p>
	 * Expected: Connection has autocommit disabled.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testAutoCommitOff() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultConnectCommand());
		
		workspace.command("sql:autocommit-off");
		
		boolean isAutoCommit = (Boolean)workspace.report("sql:autocommit-enabled?");
		assertFalse("AutoCommit should be false after sql:autocommit-off", isAutoCommit);
	}
	
	/**
	 * Test autocommit status after using sql:autocommit-on on a connection that already had autocommit enabled.
	 * <p>
	 * Expected: Connection has autocommit enabled.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testAutoCommitOn_noOp() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultConnectCommand());
		
		workspace.command("sql:autocommit-on");
		
		boolean isAutoCommit = (Boolean)workspace.report("sql:autocommit-enabled?");
		assertTrue("AutoCommit should be true after (no-op) sql:autocommit-on", isAutoCommit);
	}
	
	/**
	 * Test autocommit status after disabling using sql:autocommit-off and enabling using sql:autocommit-on.
	 * <p>
	 * Expected: Connection has autocommit enabled.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testAutoCommitOn_switchOff_and_On() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultConnectCommand());
		
		workspace.command("sql:autocommit-off");
		workspace.command("sql:autocommit-on");
		
		boolean isAutoCommit = (Boolean)workspace.report("sql:autocommit-enabled?");
		assertTrue("AutoCommit should be true after sql:autocommit-off and sql:autocommit-on", isAutoCommit);
	}

	/**
	 * Tests autocommit status for connection obtained from the connection pool. 
	 * <p>
	 * This test works in phases:
	 * <ol>
	 * <li>obtain 10 connections, checks autocommit status (expected: true), disables autocommit,
	 * checks autocommit status (expected: false)</li>
	 * <li>close 10 connections obtained previously, check for actual closure</li>
	 * <li>obtain 10 connections, check autocommit status (expected: true)</li>
	 * <li>close 10 connections obtained previously, check for actual closure</li>
	 * </ol>
	 * </p> 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testAutoCommitOn_connectionPool() throws Exception {
		workspace.open("init-sql.nlogo");
		workspace.command(getDefaultPoolConfigurationCommand());
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		
		List<Turtle> agents = new ArrayList<Turtle>();
		// Phase 1: obtain 10 connections, checks autocommit status (expected: true), disables autocommit, checks autocommit status (expected: false)
		for (int i = 0; i < 10; i++) {
			Turtle turtle = workspace.world.createTurtle(breed);
			agents.add(turtle);
			boolean isAutoCommit = (Boolean)workspace.evaluateReporter("sql:autocommit-enabled?", turtle);
			assertTrue("AutoCommit should be true for freshly retrieved pooled connection", isAutoCommit);
			workspace.evaluateCommands("sql:autocommit-off", turtle, true);
			isAutoCommit = (Boolean)workspace.evaluateReporter("sql:autocommit-enabled?", turtle);
			assertFalse("AutoCommit should be false after sql:autocommit-off", isAutoCommit);
		}
		
		// Phase 2: close 10 connections obtained previously, check for actual closure
		for (Turtle turtle : agents) {
			workspace.evaluateCommands("sql:disconnect", turtle, true);
			assertFalse("Should no longer be connected", (Boolean)workspace.evaluateReporter("sql:debug-is-connected?", turtle));
		}
		
		agents.clear();
		// Phase 3: obtain 10 connections, check autocommit status (expected: true)
		for (int i = 0; i < 10; i++) {
			Turtle turtle = workspace.world.createTurtle(breed);
			agents.add(turtle);
			boolean isAutoCommit = (Boolean)workspace.evaluateReporter("sql:autocommit-enabled?", turtle);
			assertTrue("AutoCommit should be true for freshly retrieved pooled connection", isAutoCommit);
		}
		
		// Phase 4: close 10 connections obtained previously, check for actual closure
		for (Turtle turtle : agents) {
			workspace.evaluateCommands("sql:disconnect", turtle, true);
			assertFalse("Should no longer be connected", (Boolean)workspace.evaluateReporter("sql:debug-is-connected?", turtle));
		}
	}
}
