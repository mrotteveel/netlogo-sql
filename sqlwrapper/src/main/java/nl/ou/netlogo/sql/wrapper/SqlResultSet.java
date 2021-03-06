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

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import nl.ou.netlogo.sql.wrapper.SqlConnection.AutodisconnectCoordinator;

import org.nlogo.api.*;

/**
 * SqlResultSet is a wrapper around the JDBC result set
 * 
 * @author NetLogo project-team
 * 
 */
public class SqlResultSet {

    private static final Logger LOG = SqlLogger.getLogger();

    private ResultSet resultSet;
    private ResultSetMetaData metaData;
    private boolean endOfResultSet = true;
    private boolean fetching = false;
    private final AutodisconnectCoordinator autodisconnectCoordinator;

    /**
     * Sets up the result set in a SQL like format.
     * 
     * @param resultSet
     * @param autodisconnectCoordinator
     *            AutodisconnectCoordinator
     * @throws ExtensionException
     */
    protected SqlResultSet(ResultSet resultSet, AutodisconnectCoordinator autodisconnectCoordinator)
            throws ExtensionException {
        this.resultSet = resultSet;
        this.autodisconnectCoordinator = autodisconnectCoordinator;

        try {
            if (resultSet.next()) {
                // ResultSet has at least one row
                metaData = resultSet.getMetaData();
                endOfResultSet = false;
            } else {
                autodisconnectCoordinator.endOfResultSet();
            }
        } catch (SQLException ex) {
            throw new ExtensionException(ex);
        }

    }

    /**
     * Method to check if a resultset is available.
     * 
     * @return true if so, false otherwise
     */
    public boolean isResultSetAvailable() {
        return resultSet != null;
    }

    /**
     * Method used to check if there is a next row available in the result set.
     * 
     * @return true if so, false otherwise.
     */
    public boolean isRowAvailable() {
        return isResultSetAvailable() && !isEndOfResultSet();
    }
    
    /**
     * Indicates if the end of the resultset was reached.
     * 
     * @return <code>true</code> end was reached
     */
    public boolean isEndOfResultSet() {
        return endOfResultSet;
    }

    /**
     * Indicates if results have been fetched from the resultset.
     * 
     * @return <code>true</code> a fetchRow() call has been performed
     */
    public boolean isFetching() {
        return fetching;
    }

    /**
     * @return The next row of the result set; null when past last row
     * @throws ExtensionException
     */
    public LogoList fetchRow() throws ExtensionException {
        // Set fetching to true to indicate fetchRow() is called at least once:
        fetching = true;
        if (isEndOfResultSet()) {
            return new LogoListBuilder().toLogoList();
        }

        try {
            // the metadata is used to do the convert datatypes from SQL to NetLogo
            LOG.log(Level.FINE, "SqlResultSet.fetchRow(): dataconversion");
            LogoListBuilder cols = new LogoListBuilder();

            for (int i = 1; i <= metaData.getColumnCount(); ++i) {
                switch (metaData.getColumnType(i)) {
                case java.sql.Types.CHAR:
                case java.sql.Types.VARCHAR:
                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                case java.sql.Types.LONGVARBINARY:
                case java.sql.Types.TIME:
                case java.sql.Types.DATE:
                case java.sql.Types.TIMESTAMP:
                    cols.add(resultSet.getString(i));
                    break;
                case java.sql.Types.TINYINT:
                case java.sql.Types.SMALLINT:
                case java.sql.Types.INTEGER:
                case java.sql.Types.BIGINT:
                case java.sql.Types.NUMERIC:
                case java.sql.Types.DECIMAL:
                case java.sql.Types.REAL:
                case java.sql.Types.FLOAT:
                case java.sql.Types.DOUBLE:
                    cols.add(resultSet.getDouble(i));
                    break;
                case java.sql.Types.BIT:
                case java.sql.Types.BOOLEAN:
                    cols.add(resultSet.getBoolean(i));
                    break;
                }
            }

            if (!resultSet.next()) {
                endOfResultSet = true;
                autodisconnectCoordinator.endOfResultSet();
            }

            return cols.toLogoList();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return list of rows
     * @throws ExtensionException
     */
    public LogoList fetchResultSet() throws ExtensionException {
        LogoListBuilder rows = new LogoListBuilder();

        // Return empty list if there is no row available, if something has already been fetched
        if (!isRowAvailable() || isFetching()) {
            return rows.toLogoList();
        }

        LogoList cols;
        while (isRowAvailable()) {
            cols = fetchRow();
            rows.add(cols);
        }
        return rows.toLogoList();
    }

    /**
     * Method used to close a result and release resources on database engine.
     */
    public void close() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.warning("Exception while closing result set: " + e);
                // ignore: result set will not be used anymore
            } finally {
                resultSet = null;
                metaData = null;
            }
        }
    }
}
