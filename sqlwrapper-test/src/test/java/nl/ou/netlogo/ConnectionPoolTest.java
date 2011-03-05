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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.Ignore;
import org.nlogo.agent.AgentSet;
import org.nlogo.agent.Turtle;
import org.nlogo.api.CompilerException;
import org.nlogo.api.LogoList;
import org.nlogo.nvm.EngineException;

import nl.ou.netlogo.testsupport.ConnectionInformation;
import nl.ou.netlogo.testsupport.HeadlessTest;
import static nl.ou.netlogo.testsupport.DatabaseHelper.getDefaultPoolConfigurationCommand;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test some aspects of connection pooling. Other tests related to connection pooling may reside in the test-classes for
 * the specific language primitives.
 * <p>
 * Note that the connectionpool settings partitions and min-connections are not tested as there is no viable way
 * to test this.
 * </p>
 * 
 * @author Mark Rotteveel
 */
public class ConnectionPoolTest extends HeadlessTest {
	
	private static final int DEFAULT_TIMEOUT_SECONDS = 5;
	private static final int DEFAULT_MAX_CONNECTIONS = 20;
	
	private ExecutorService executor;
	
	/**
	 * Test if a connection pool returns distinct connections for agents (in this case: observer and one turtle).
	 * <p>
	 * Expected: two distinct connections are returned.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_distinctConnections() throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		Turtle turtle = workspace.world.createTurtle(breed);
		assertNotNull("Unable to create turtle", turtle);
		
		workspace.command(getDefaultPoolConfigurationCommand());
		
		workspace.command("sql:exec-direct \"SELECT CAST(connection_id() AS CHAR)\"");
		LogoList observerRow = (LogoList)workspace.report("sql:fetch-row");
		String observerConnection = (String)observerRow.get(0);
		
		workspace.evaluateCommands("sql:exec-direct \"SELECT CAST(connection_id() AS CHAR)\"", turtle, true);
		LogoList agentRow = (LogoList)workspace.evaluateReporter("sql:fetch-row", turtle);
		String agentConnection = (String)agentRow.get(0);
		
		assertFalse("Agent and observer should have different connections", observerConnection.equals(agentConnection));
	}
	
	/**
	 * Test if using sql:connect command when database pooling is enabled will return a distinct connection
	 * that is not from the connection pool.
	 * <p>
	 * Expected: Returned connection is not from the connection pool.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_useConnectCommand() throws Exception {
		workspace.open("init-sql.nlogo"); 
	
		workspace.command(getDefaultPoolConfigurationCommand());
		ConnectionInformation ci = ConnectionInformation.getInstance();
		workspace.command(String.format("sql:connect [[\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"]]",
				ci.getHost(), ci.getPort(), 
				ci.getUsername(), ci.getPassword(), "information_schema"));
		String observerDB = (String)workspace.report("sql:current-database");
		assertEquals("Observer does not have expected database", "information_schema", observerDB);
	}
	
	/**
	 * Test if using sql:connect command for one agent, does not influence connections obtained from the
	 * connection pool.
	 * <p>
	 * Expected: Target database of connection pool is not changed by using sql:connect for another agent.
	 * </p>
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_useConnectCommand_checkOtherConnections() throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
	
		workspace.command(getDefaultPoolConfigurationCommand());
		ConnectionInformation ci = ConnectionInformation.getInstance();
		workspace.command(String.format("sql:connect [[\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"]]",
				ci.getHost(), ci.getPort(), 
				ci.getUsername(), ci.getPassword(), "information_schema"));
		String observerDB = (String)workspace.report("sql:current-database");
		assertEquals("Observer does not have expected database", "information_schema", observerDB);
		
		for (int i = 0; i < 10; i++) {
			System.out.println("Creating agent " + i);
			Turtle turtle = workspace.world.createTurtle(breed);
			assertNotNull("Unable to create turtle", turtle);
			String agentDB = (String)workspace.evaluateReporter("sql:current-database", turtle);
			assertFalse("Observer and agent should have different database", observerDB.equals(agentDB));
			assertEquals("Schema should be default test schema", ci.getSchema(), agentDB);
		}
	}
	
	/**
	 * Test for the connection timeout in the default configuration.
	 * <p>
	 * Assumptions:
	 * <ul>
	 * <li>timeout = 5 seconds (default)</li>
	 * <li>max-connections = 20</li>
	 * <li>autodisconnect = off</li>
	 * </ul>
	 * </p>
	 * @throws Exception For any exceptions during testing
	 */
	@Ignore
	public void testConnectionPool_connectionTimeout_default() throws Exception {
		timeoutTest(DEFAULT_TIMEOUT_SECONDS);
	}
	
	/**
	 * Actual timeout test.
	 * 
	 * @param timeout Expected timeout in seconds
	 * @throws Exception For any exceptions during testing
	 */
	private void timeoutTest(int timeout) throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		
		if (timeout != DEFAULT_TIMEOUT_SECONDS) {
			workspace.command(String.format("sql:configure \"connectionpool\" [[\"timeout\" \"%d\"]]", timeout));
		}
		
		executor = Executors.newSingleThreadExecutor();
		try {
			allocateMaxConnections(breed);
	
			long startTime = System.currentTimeMillis();
			Future<Object> future = executor.submit(new Getter(DEFAULT_MAX_CONNECTIONS, breed));
			int timeoutMillis = timeout * 1000 + 600;
			Object result = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
			long endTime = System.currentTimeMillis();
			assertEquals(String.format("Expected timout of %d ms (+/- 500 ms)", timeout * 1000), timeout * 1000, endTime - startTime, 500);
			assertTrue("Expected exception as a result", result instanceof Exception);
			assertEquals("Extension exception: connectionPool.createConnectionFromPool() timed out", ((Exception)result).getMessage());
		} finally {
			executor.shutdownNow();
		}
	}

	/**
	 * Allocate the maximum number of connections from the connection pool.
	 * <p>
	 * This method assumes default max connections of 20.
	 * </p>
	 * 
	 * @param breed Breed of agent
	 * @return Maximum number of connections supported by pool 
	 * @throws Exception For any exceptions during testing
	 */
	private void allocateMaxConnections(AgentSet breed) throws Exception {
		workspace.command(getDefaultPoolConfigurationCommand(false));
		System.out.printf("Expecting to be able to allocate %d connections%n", DEFAULT_MAX_CONNECTIONS);
		ConnectionInformation ci = ConnectionInformation.getInstance();
		for (int i = 0; i < DEFAULT_MAX_CONNECTIONS; i++) {
			Future<Object> future = executor.submit(new Getter(i, breed));
			Object result = null;
			try {
				result = future.get(10, TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				future.cancel(true);
				fail(String.format("Retrieving connection %d failed, expected to be able to allocate upto %d", i, DEFAULT_MAX_CONNECTIONS - 1));
			}  catch (ExecutionException e) {
				future.cancel(true);
				fail(String.format("Retrieving connection %d failed, expected to be able to allocate upto %d", i, DEFAULT_MAX_CONNECTIONS - 1));
			}
			assertEquals("Expected result to be a string", String.class, result.getClass());
			assertEquals("Schema should be default test schema", ci.getSchema(), result);
		}
	}
	
	/**
	 * Test for the connection timeout in the default configuration.
	 * <p>
	 * Assumptions:
	 * <ul>
	 * <li>max-connections = 20</li>
	 * <li>autodisconnect = off</li>
	 * </ul>
	 * </p>
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_connectionTimeout_10seconds() throws Exception {
		timeoutTest(10);
	}
	
	/**
	 * Test for the connection timeout in the default configuration.
	 * <p>
	 * Assumptions:
	 * <ul>
	 * <li>max-connections = 20</li>
	 * <li>autodisconnect = off</li>
	 * </ul>
	 * </p>
	 * @throws Exception For any exceptions during testing
	 */
	@Test(timeout = 20000, expected = TimeoutException.class)
	public void testConnectionPool_noConnectionTimeout() throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		
		executor = Executors.newSingleThreadExecutor();
		try {
			workspace.command("sql:configure \"connectionpool\" [[\"timeout\" \"0\"]]");
			allocateMaxConnections(breed);
			
			Future<Object> future = executor.submit(new Getter(DEFAULT_MAX_CONNECTIONS, breed));
		
			try {
				future.get(10, TimeUnit.SECONDS);
			} finally {
				future.cancel(true);
			}
		} finally {
			executor.shutdownNow();
		}
	}
	
	/**
	 * Test for the connectionpool max-connections setting. By configuring the max-connections values lower than the default maximum connections, 
	 * the number of connections should be limited to the value of max-connections.
	 * <p>
	 * Expected: only max-connections (= 10 in this test) connections are allocated.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_maxConnections_lowerThanDefault() throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		int testMax = 10;
		workspace.command(String.format("sql:configure \"connectionpool\" [[\"max-connections\" %d]]", testMax));
		workspace.command(getDefaultPoolConfigurationCommand(false));
		
		ensureMaxConnections(breed, testMax);
	}
	
	/**
	 * Test for the connectionpool max-connections setting. By configuring the max-connections values higher than the default maximum connections, 
	 * the number of connections should be limited to the value of max-connections.
	 * <p>
	 * Expected: only max-connections (= 30 in this test) connections are allocated.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_maxConnections_higherThanDefault() throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		int testMax = 30;
		workspace.command(String.format("sql:configure \"connectionpool\" [[\"max-connections\" %d]]", testMax));
		workspace.command(getDefaultPoolConfigurationCommand(false));
		
		ensureMaxConnections(breed, testMax);
	}
	
	// TODO Tests below may need to be changed if connectionsPerPartition limit changes
	
	/**
	 * Test for the connectionpool max-connections setting. If configured at 0, should throw an error.
	 * <p>
	 * Expected: exception when configuring to have no connection.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test(expected=EngineException.class)
	public void testConnectionPool_maxConnections_zero() throws Exception {
		workspace.open("init-sql.nlogo");
		int testMax = 0;
		workspace.command(String.format("sql:configure \"connectionpool\" [[\"max-connections\" %d]]", testMax));
		workspace.command(getDefaultPoolConfigurationCommand());
	}
	
	/**
	 * Test for the connectionpool max-connections setting. If configured at 5, should work.
	 * <p>
	 * Expected: able to allocate 5 connections
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testConnectionPool_maxConnections_five() throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		
		int testMax = 5;
		workspace.command(String.format("sql:configure \"connectionpool\" [[\"max-connections\" %d]]", testMax));
		workspace.command(getDefaultPoolConfigurationCommand(false));
		
		ensureMaxConnections(breed, testMax);
	}
	
	/**
	 * Test for the connectionpool max-connections setting. If configured at 4, should throw exception because of connection limit at 5.
	 * <p>
	 * Expected: not be able to allocate 4 connections
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test(expected=EngineException.class)
	public void testConnectionPool_maxConnections_lessThanFive() throws Exception {
		workspace.open("init-sql.nlogo");
		AgentSet breed = workspace.world.getBreed("TESTAGENT");
		assertNotNull("Breed TESTAGENT not defined in workspace", breed);
		
		int testMax = 4;
		workspace.command(String.format("sql:configure \"connectionpool\" [[\"max-connections\" %d]]", testMax));
		workspace.command(getDefaultPoolConfigurationCommand(false));
		
		ensureMaxConnections(breed, testMax);
	}
	
	/**
	 * Test for the connectionpool max-connections and partitions setting. If configured 
	 * max-connections/partitions < 5, should throw an error.
	 * <p>
	 * Expected: exception when configuring to have no connection per partition.
	 * </p>
	 * <p>
	 * TODO: Enable/change test after discussion is resolved.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Ignore("currently limit is on total connections, not connections per partition, waiting on e-mail discussion")
	@Test(expected=EngineException.class)
	public void testConnectionPool_connectionsPerPartition_lessThanFive() throws Exception {
		workspace.open("init-sql.nlogo");
		int testMax = 9;
		int partitons = 2;
		workspace.command(String.format("sql:configure \"connectionpool\" [[\"max-connections\" %d] [\"partitions\" %d]]", testMax, partitons));
		workspace.command(getDefaultPoolConfigurationCommand());
	}
	
	/**
	 * Test for the connectionpool partitions setting. If configured at 0, should throw an error.
	 * <p>
	 * Expected: exception when configuring to have no connection.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test(expected=EngineException.class)
	public void testConnectionPool_partitions_zero() throws Exception {
		workspace.open("init-sql.nlogo");
		int testMax = 0;
		workspace.command(String.format("sql:configure \"connectionpool\" [[\"partitions\" %d]]", testMax));
		workspace.command(getDefaultPoolConfigurationCommand());
	}
	
	/**
	 * Helper method to check maximum number of connections that can be allocated.
	 * 
	 * @param breed Agent breed
	 * @param expectedMax Expected number of connections that should be allocatable.
	 * @throws Exception For any exceptions during testing
	 */
	private void ensureMaxConnections(AgentSet breed, int expectedMax) throws Exception {
		
		executor = Executors.newSingleThreadExecutor();
		try {
			for (int i = 0; i < expectedMax; i++) {
				Future<Object> future = executor.submit(new Getter(i, breed));
				try {
					Object result = future.get(10, TimeUnit.SECONDS);
					assertEquals("Expected String result", String.class, result.getClass());
				} catch (Exception e) {
					future.cancel(true);
					fail(String.format("Failed acquiring connection %d, exception: %s", i, e.getMessage()));
				}
			}
			// Now acquired configured maximum, next retrieved connection should timeout
			Future<Object> future = executor.submit(new Getter(expectedMax, breed));
			try {
				Object result = future.get(10, TimeUnit.SECONDS);
				System.out.println(result);
				assertEquals("Expected EngineException result", EngineException.class, result.getClass());
				assertEquals("Unexpected exception message", "Extension exception: connectionPool.createConnectionFromPool() timed out", 
						((Exception)result).getMessage());
				System.out.println(result);
			} catch (Exception e) {
				future.cancel(true);
				fail(String.format("Unexpected executor exception for connection %d, exception: %s", expectedMax, e.getMessage()));
			} 
		} finally {
			executor.shutdownNow();
		}
	}
	
	
	/**
	 * Callable to retrieve connections and enable timeout. 
	 */
	private class Getter implements Callable<Object> {
		
		private final int connectionCount;
		private final AgentSet breed;
		
		/**
		 * Create getter
		 * @param connectionCount identifier (count) for the agent
		 * @param breed Breed of the agent
		 */
		private Getter(int connectionCount, AgentSet breed) {
			this.connectionCount = connectionCount;
			this.breed = breed;
		}

		@Override
		public Object call() throws Exception {
			System.out.println("Creating agent " + connectionCount);
			Turtle turtle = workspace.world.createTurtle(breed);
			try {
				return workspace.evaluateReporter("sql:current-database", turtle);
			} catch (CompilerException e) {
				return null;
			}
		}
	}
}
