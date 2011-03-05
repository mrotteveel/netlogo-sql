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
package nl.ou.netlogo.sql.wrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import nl.ou.netlogo.sql.wrapper.SqlConnection.ConnectionEvent;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Test;

public class SqlConnectionTest {
	
	private Mockery context = new JUnit4Mockery();
	
	/**
	 * Test if {@link SqlConnection#close()} notifies all registered observers.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testClose_notifiesObservers() {
		final EventObserver<ConnectionEvent> observer1 = context.mock(EventObserver.class, "observer1");
		final EventObserver<ConnectionEvent> observer2 = context.mock(EventObserver.class, "observer2");
		
		final SqlConnection sqlCon = new SqlConnection(null, null);
		sqlCon.register(observer1);
		sqlCon.register(observer2);
		
		context.checking(new Expectations() {{
			oneOf(observer1).notify(ConnectionEvent.CLOSE, sqlCon);
			oneOf(observer2).notify(ConnectionEvent.CLOSE, sqlCon);
		}});
		
		sqlCon.close();
		
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#close()} closes the physical connection.
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testClose_closesPhysicalConnection() throws Exception {
		final Connection connection = context.mock(Connection.class);
		
		SqlConnection sqlCon = new SqlConnection(connection, null);
		
		context.checking(new Expectations() {{
			oneOf(connection).close();
		}});
		
		sqlCon.close();
		
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#close()} closes the SqlStatement (and associated PreparedStatement).
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testClose_closesStatement() throws Exception {
		final Connection connection = context.mock(Connection.class);
		final PreparedStatement stmt = context.mock(PreparedStatement.class);
		final String sqlTest = "SELECT * FROM TEST";
		
		SqlConnection sqlCon = new SqlConnection(connection, null);
		context.checking(new Expectations() {{
			oneOf(connection).prepareStatement(sqlTest); will(returnValue(stmt));
			oneOf(stmt).close();
			// Don't care about connection close in this test
			allowing(connection).close();
		}});
		sqlCon.createStatement(sqlTest);
		
		sqlCon.close();
		
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#autoDisconnect()} notifies all registered observers when called.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testAutoDisconnect_notifiesObservers() {
		final DatabaseInfo dbInfo = context.mock(DatabaseInfo.class);
		final EventObserver<ConnectionEvent> observer1 = context.mock(EventObserver.class, "observer1");
		final EventObserver<ConnectionEvent> observer2 = context.mock(EventObserver.class, "observer2");
		
		final SqlConnection sqlCon = new SqlConnection(null, dbInfo);
		sqlCon.register(observer1);
		sqlCon.register(observer2);
		
		context.checking(new Expectations() {{
			oneOf(observer1).notify(ConnectionEvent.AUTO_DISCONNECT, sqlCon);
			oneOf(observer2).notify(ConnectionEvent.AUTO_DISCONNECT, sqlCon);
		}});
		
		sqlCon.autoDisconnect();
		
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#notify(nl.ou.netlogo.sql.wrapper.SqlConnection.AutodisconnectEvent, EventObservable)()} notifies all registered observers 
	 * when autodisconnect is enabled.
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testAutoDisconnect_enabled_notifiesObservers() throws Exception {
		final DatabaseInfo dbInfo = context.mock(DatabaseInfo.class);
		final EventObserver<ConnectionEvent> observer1 = context.mock(EventObserver.class, "observer1");
		final EventObserver<ConnectionEvent> observer2 = context.mock(EventObserver.class, "observer2");
		final Connection connection = context.mock(Connection.class);
		
		final SqlConnection sqlCon = new SqlConnection(connection, dbInfo);
		sqlCon.register(observer1);
		sqlCon.register(observer2);
		
		context.checking(new Expectations() {{
			oneOf(dbInfo).useAutoDisconnect(); will(returnValue(true));
			oneOf(connection).getAutoCommit(); will(returnValue(true));
			ignoring(connection);
			oneOf(observer1).notify(ConnectionEvent.AUTO_DISCONNECT, sqlCon);
			oneOf(observer2).notify(ConnectionEvent.AUTO_DISCONNECT, sqlCon);
		}});
		
		sqlCon.autodisconnectCoordinator.endOfResultSet();
		
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#notify(nl.ou.netlogo.sql.wrapper.SqlConnection.AutodisconnectEvent, EventObservable)()} does not notify 
	 * registered observers when autodisconnect is disabled.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testAutoDisconnect_disabled_noNotifyObservers() {
		final DatabaseInfo dbInfo = context.mock(DatabaseInfo.class);
		final EventObserver<ConnectionEvent> observer1 = context.mock(EventObserver.class, "observer1");
		final EventObserver<ConnectionEvent> observer2 = context.mock(EventObserver.class, "observer2");
		
		final SqlConnection sqlCon = new SqlConnection(null, dbInfo);
		sqlCon.register(observer1);
		sqlCon.register(observer2);
		
		context.checking(new Expectations() {{
			oneOf(dbInfo).useAutoDisconnect(); will(returnValue(false));
			never(observer1).notify(ConnectionEvent.AUTO_DISCONNECT, sqlCon);
			never(observer2).notify(ConnectionEvent.AUTO_DISCONNECT, sqlCon);
		}});
		
		sqlCon.autodisconnectCoordinator.endOfResultSet();
		
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if autodisconnect does not close the physical connection when autodisconnect is disabled.
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testAutodisconnect_disabled_noClosePhysicalConnection() throws Exception {
		final DatabaseInfo dbInfo = context.mock(DatabaseInfo.class);
		final Connection connection = context.mock(Connection.class);
		
		final SqlConnection sqlCon = new SqlConnection(connection, dbInfo);
		
		context.checking(new Expectations() {{
			oneOf(dbInfo).useAutoDisconnect(); will(returnValue(false));
			never(connection).close();
		}});
		
		sqlCon.autodisconnectCoordinator.endOfResultSet();
		
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if autodisconnect closes the physical connection when called.
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testAutodisconnect_closesPhysicalConnection() throws Exception {
		final DatabaseInfo dbInfo = context.mock(DatabaseInfo.class);
		final Connection connection = context.mock(Connection.class);
		
		final SqlConnection sqlCon = new SqlConnection(connection, dbInfo);
		
		context.checking(new Expectations() {{
			oneOf(connection).close();
		}});
		
		sqlCon.autoDisconnect();
		
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if autodisconnect does not close the SqlStatement (and associated PreparedStatement).
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testAutodisconnect_enabled_noCloseStatement() throws Exception {
		final DatabaseInfo dbInfo = context.mock(DatabaseInfo.class);
		final Connection connection = context.mock(Connection.class);
		final PreparedStatement stmt = context.mock(PreparedStatement.class);
		final String sqlTest = "SELECT * FROM TEST";
		
		final SqlConnection sqlCon = new SqlConnection(connection, dbInfo);
		context.checking(new Expectations() {{
			oneOf(connection).prepareStatement(sqlTest); will(returnValue(stmt));
			never(stmt).close();
			// Don't care about connection close in this test
			allowing(connection).close();
		}});
		
		sqlCon.createStatement(sqlTest);
		sqlCon.autoDisconnect();
		
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#isConnected()} returns false when there is no physical connection.
	 */
	@Test
	public void testIsConnected_noConnection() {
		SqlConnection sqlCon = new SqlConnection(null, null);
		
		assertFalse("Expected false for isConnected() without physical connection", sqlCon.isConnected());
	}
	
	/**
	 * Test if {@link SqlConnection#isConnected()} returns true when the physical connection is not closed.
	 */
	@Test
	public void testIsConnected_connectionOpen() throws Exception {
		final Connection connection = context.mock(Connection.class);
		
		final SqlConnection sqlCon = new SqlConnection(connection, null);
		
		context.checking(new Expectations() {{
			oneOf(connection).isClosed(); will(returnValue(false));
		}});
		
		assertTrue("Expected isConnected() to return true when physical connection is open", sqlCon.isConnected());
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#isConnected()} returns false when the physical connection is closed.
	 */
	@Test
	public void testIsConnected_connectionClosed() throws Exception {
		final Connection connection = context.mock(Connection.class);
		
		final SqlConnection sqlCon = new SqlConnection(connection, null);
		
		context.checking(new Expectations() {{
			oneOf(connection).isClosed(); will(returnValue(true));
		}});
		
		assertFalse("Expected isConnected() to return false when physical connection is closed", sqlCon.isConnected());
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#isConnected()} returns false when the physical connection throws an exception for isClosed().
	 */
	@Test
	public void testIsConnected_connectionException() throws Exception {
		final Connection connection = context.mock(Connection.class);
		
		final SqlConnection sqlCon = new SqlConnection(connection, null);
		
		context.checking(new Expectations() {{
			oneOf(connection).isClosed(); will(throwException(new SQLException()));
		}});
		
		assertFalse("Expected isConnected() to return false when physical connection throws SQLException", sqlCon.isConnected());
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#getResultSet()} returns null if there is no SqlStatement
	 */
	@Test
	public void testGetResultSet_noStatement() {
		SqlConnection sqlCon = new SqlConnection(null, null);
		
		assertNull("Expected null for getResultSet()", sqlCon.getResultSet());
	}
	
	// Further behavior of getResultSet is part of SqlStatement testing
	
	/**
	 * Test if {@link SqlConnection#getRowCount()} returns -1.0 if there is no SqlStatement 
	 */
	@Test
	public void testGetRowCount_noStatement() {
		SqlConnection sqlCon = new SqlConnection(null, null);
		
		assertEquals("Unexpected value for getRowCount without connection", Double.valueOf(-1), sqlCon.getRowCount());
	}
	
	/**
	 * Test if {@link SqlConnection#getRowCount()} returns the right (double) value of the updated row count, and no error
	 * occurs during value conversion.
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testGetRowCount_doubleConversion() throws Exception {
		final DatabaseInfo dbInfo = context.mock(DatabaseInfo.class);
		final Connection connection = context.mock(Connection.class);
		final PreparedStatement stmt = context.mock(PreparedStatement.class);
		final String sqlTest = "SELECT * FROM TEST";
		final int updateCount = 513;
		
		SqlConnection sqlCon = new SqlConnection(connection, dbInfo);
		
		context.checking(new Expectations() {{
			ignoring(dbInfo);
			oneOf(connection).prepareStatement(sqlTest); will(returnValue(stmt));
			oneOf(stmt).execute(); will(returnValue(false));
			oneOf(stmt).getUpdateCount(); will(returnValue(updateCount));
		}});
		
		SqlStatement sqlStmt = sqlCon.createStatement(sqlTest);
		sqlStmt.executeDirect();
		
		assertEquals("Unexpected value for getRowCount()", Double.valueOf(updateCount), sqlCon.getRowCount());
		context.assertIsSatisfied();
	}
	
	/**
	 * Test if {@link SqlConnection#createStatement(String, org.nlogo.api.LogoList) closes the previously opened statement.
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testCreateStatement_closesPreviousStatement() throws Exception {
		final Connection connection = context.mock(Connection.class);
		final PreparedStatement firstStmt = context.mock(PreparedStatement.class, "firstStmt");
		final PreparedStatement secondStmt = context.mock(PreparedStatement.class, "secondStmt");
		final Sequence seq = context.sequence("execution_sequence");
		final String sqlTest = "SELECT * FROM TEST";
		
		SqlConnection sqlCon = new SqlConnection(connection, null);
		
		context.checking(new Expectations() {{
			oneOf(connection).prepareStatement(sqlTest); will(returnValue(firstStmt)); inSequence(seq);
			oneOf(firstStmt).close(); inSequence(seq);
			oneOf(connection).prepareStatement(sqlTest); will(returnValue(secondStmt)); inSequence(seq);
		}});
		
		sqlCon.createStatement(sqlTest, null);
		sqlCon.createStatement(sqlTest, null);
		
		context.assertIsSatisfied();
	}
	
}
