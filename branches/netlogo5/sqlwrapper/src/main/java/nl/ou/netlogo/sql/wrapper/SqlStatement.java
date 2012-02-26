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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.sql.*;

import nl.ou.netlogo.sql.wrapper.SqlConnection.AutodisconnectCoordinator;

import org.nlogo.api.*;

/**
 * SqlStatement implements a wrapper for the JDBC PreparedStatement class.
 * 
 * @author NetLogo project-team
 * 
 */
public class SqlStatement {

    private static final Logger LOG = SqlLogger.getLogger();

    private PreparedStatement statement;
    private LogoList parameters;
    private SqlResultSet resultSet;
    private int rowCount = -1;
    private final AutodisconnectCoordinator autodisconnectCoordinator;

    /**
     * Constructor for SqlStatement with support for parameters (parameterized
     * queries).
     * 
     * @param statement
     *            PreparedStatement object
     * @param parameters
     *            Parameters as a NetLogo list (or null or empty list if no
     *            parameters are needed)
     * @param autodisconnectCoordinator
     *            AutodisconnectCoordinator
     */
    protected SqlStatement(PreparedStatement statement, LogoList parameters,
            AutodisconnectCoordinator autodisconnectCoordinator) {
        this.statement = statement;
        this.parameters = parameters;
        this.autodisconnectCoordinator = autodisconnectCoordinator;
    }

    /**
     * Executes an direct SQL statement through the JDBC interface.
     * 
     * @return boolean true means result set available; false means row count
     *         available
     * @throws ExtensionException
     * @see #getResultSet
     * @see #getRowCount
     */
    public boolean executeDirect() throws ExtensionException {
        LOG.log(Level.FINE, "SqlStatement.executeDirect('{0}')", new Object[] { statement });
        try {
            if (statement.execute()) {
                // expect a result set
                resultSet = new SqlResultSet(statement.getResultSet(), autodisconnectCoordinator);
                return true;
            } else {
                // expect an update count
                rowCount = statement.getUpdateCount();
                autodisconnectCoordinator.noResultSet();
                return false;
            }
        } catch (Exception e) {
            close();
            if (e instanceof ExtensionException) {
                throw (ExtensionException) e;
            }
            throw new ExtensionException(e);
        }
    }

    /**
     * Executes a prepared query statement through the JDBC interface.
     * 
     * @throws ExtensionException
     */
    public void executeQuery() throws ExtensionException {
        LOG.log(Level.FINE, "SqlStatement.executeQuery('{0}', '{1}')", new Object[] { statement, parameters });
        try {
            prepareStatement();
            resultSet = new SqlResultSet(statement.executeQuery(), autodisconnectCoordinator);
        } catch (Exception e) {
            close();
            if (e instanceof ExtensionException) {
                throw (ExtensionException) e;
            }
            throw new ExtensionException(e);
        }
    }

    /**
     * Executes a prepared update statement (eg INSERT, UPDATE, DELETE) through
     * the JDBC interface.
     * 
     * @throws ExtensionException
     */
    public void executeUpdate() throws ExtensionException {
        LOG.log(Level.FINE, "SqlStatement.executeUpdate('{0}', '{1}')", new Object[] { statement, parameters });
        try {
            prepareStatement();
            rowCount = statement.executeUpdate();
            autodisconnectCoordinator.noResultSet();
        } catch (Exception e) {
            close();
            if (e instanceof ExtensionException) {
                throw (ExtensionException) e;
            }
            throw new ExtensionException(e);
        }
    }

    /**
     * Prepares the statement by setting the parameters.
     * 
     * @throws SQLException
     * @throws ExtensionException
     */
    protected void prepareStatement() throws SQLException, ExtensionException {
        ParameterMetaData parameterMetaData = statement.getParameterMetaData();
        int parameterCount = parameterMetaData.getParameterCount();
        if ((parameters == null && parameterCount != 0) || (parameters != null && parameterCount != parameters.size())) {
            throw new ExtensionException(String.format(
                    "Incorrect number of query parameters passed, expected %d received %d", parameterCount,
                    parameters == null ? 0 : parameters.size()));
        }
        
        if (parameters != null) {
            for (int idx = 0; idx < parameters.size(); idx++) {
                Object parameter = parameters.get(idx);
                processParameter(parameter, idx);
            }
        }
    }

    /**
     * Processes the parameter and assigns it to the PreparedStatement.
     * 
     * @param parameter
     *            Parameter to process
     * @param idx
     *            Index of the parameter (0-based!)
     * @throws SQLException
     * @throws ExtensionException
     */
    private void processParameter(Object parameter, int idx) throws SQLException, ExtensionException {
        // JDBC parameters are 1-based:
        idx = idx + 1;
        if (parameter instanceof String) {
            statement.setString(idx, (String) parameter);
        } else if (parameter instanceof Double) {
            // TODO check if SQL type specific conversions are required, or if default JDBC conversion is sufficient
            statement.setDouble(idx, (Double) parameter);
        } else if (parameter instanceof Boolean) {
            statement.setBoolean(idx, (Boolean) parameter);
        } else {
            // TODO check if NetLogo uses different types (eg for literal integers in the model)
            throw new ExtensionException(String.format("Unknown or unexpected parameter type %s", parameter.getClass()
                    .getName()));
        }
    }

    /**
     * Closes the statement, releases resources.
     */
    public void close() {
        LOG.log(Level.FINE, "SqlStatement.close()");
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (Exception e) {
            // ignore: resultset will not be used anymore
        } finally {
            resultSet = null;
        }
        try {
            statement.close();
        } catch (Exception e) {
            // ignore: statement will not be used anymore
        } finally {
            statement = null;
        }
    }

    /**
     * @return the result set generated by executing the statement
     */
    public SqlResultSet getResultSet() {
        return resultSet;
    }

    /**
     * @return the number of rows affected by executing the statement. The value
     *         -1 indicates that no row count is available
     */
    public int getRowCount() {
        return rowCount;
    }
}
