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
package nl.ou.netlogo.testsupport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatabaseHelper {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static Connection getConnection() throws SQLException {
        ConnectionInformation instance = ConnectionInformation.getInstance();
        return DriverManager.getConnection(getMySQLJdbcURL(), instance.getUsername(), instance.getPassword());
    }

    /**
     * 
     * @return JDBC URL for the database
     */
    public static String getMySQLJdbcURL() {
        ConnectionInformation instance = ConnectionInformation.getInstance();
        return String.format("jdbc:mysql://%s:%s/%s", instance.getHost(), instance.getPort(), instance.getSchema());
    }

    /**
     * Creates the default sql:connect command for a valid connection using the
     * (default) MySQL brand.
     * 
     * @return sql:connect command
     */
    public static String getMySQLConnectCommand() {
        ConnectionInformation instance = ConnectionInformation.getInstance();
        return String.format("sql:connect [[\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"]]",
                        instance.getHost(), instance.getPort(), instance.getUsername(), instance.getPassword(),
                        instance.getSchema());
    }

    /**
     * Creates the default sql:connect command for a valid connection using the
     * generic brand.
     * 
     * @return sql:connect command
     */
    public static String getGenericConnectCommand() {
        ConnectionInformation instance = ConnectionInformation.getInstance();
        return String.format("sql:connect [[\"brand\" \"generic\"] [\"jdbc-url\" \"%s\"] [\"driver\" \"com.mysql.jdbc.Driver\"] [\"user\" \"%s\"] [\"password\" \"%s\"]]",
                        getMySQLJdbcURL(), instance.getUsername(), instance.getPassword());
    }

    /**
     * Creates the default sql:set-default command for a valid pool
     * configuration using the (default) MySQL brand. Autodisconnect is not
     * explicitly set for this command and uses the plugin default. Use
     * {@link #getMySQLPoolConfigurationCommand(boolean)} for explicit control.
     * 
     * @return sql:set-default "connect" command
     */
    public static String getMySQLPoolConfigurationCommand() {
        ConnectionInformation instance = ConnectionInformation.getInstance();
        return String.format("sql:configure \"defaultconnection\" [[\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"]]", 
                instance.getHost(), instance.getPort().toString(), instance.getUsername(), instance.getPassword(), instance.getSchema());
    }

    /**
     * Creates the default sql:set-default command for a valid pool
     * configuration using the generic brand. Autodisconnect is not explicitly
     * set for this command and uses the plugin default. Use
     * {@link #getGenericPoolConfigurationCommand(boolean)} for explicit
     * control.
     * 
     * @return sql:set-default "connect" command
     */
    public static String getGenericPoolConfigurationCommand() {
        ConnectionInformation instance = ConnectionInformation.getInstance();
        return String.format("sql:configure \"defaultconnection\" [[\"brand\" \"generic\"] [\"jdbc-url\" \"%s\"] [\"driver\" \"com.mysql.jdbc.Driver\"] [\"user\" \"%s\"] [\"password\" \"%s\"]]", 
                getMySQLJdbcURL(), instance.getUsername(), instance.getPassword());
    }

    /**
     * Creates the default sql:set-default command for a valid pool
     * configuration using the (default) MySQL brand, and allows for switching
     * autodisconnect on/off.
     * 
     * @return sql:set-default "connect" command
     */
    public static String getMySQLPoolConfigurationCommand(boolean autodisconnect) {
        ConnectionInformation instance = ConnectionInformation.getInstance();
        return String.format("sql:configure \"defaultconnection\" [[\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"] [\"autodisconnect\" \"%s\"]]",
                instance.getHost(), instance.getPort().toString(), instance.getUsername(), instance.getPassword(),
                instance.getSchema(), autodisconnect ? "on" : "off");
    }

    /**
     * Creates the default sql:set-default command for a valid pool
     * configuration using the generic brand, and allows for switching
     * autodisconnect on/off.
     * 
     * @return sql:set-default "connect" command
     */
    public static String getGenericPoolConfigurationCommand(boolean autodisconnect) {
        ConnectionInformation instance = ConnectionInformation.getInstance();
        return String.format("sql:configure \"defaultconnection\" [[\"brand\" \"generic\"] [\"jdbc-url\" \"%s\"] [\"driver\" \"com.mysql.jdbc.Driver\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"autodisconnect\" \"%s\"]]", getMySQLJdbcURL(),
                        instance.getUsername(), instance.getPassword(), autodisconnect ? "on" : "off");
    }

    /**
     * Method to execute DDL or DML (but not SELECT) statements.
     * 
     * @param statements
     *            Statements to execute
     * @throws SQLException
     *             For any error executing the statements.
     */
    public static void executeUpdate(String... statements) throws SQLException {
        Connection con = null;
        Statement stmt = null;
        try {
            con = getConnection();
            con.setAutoCommit(false);
            stmt = con.createStatement();
            for (String statement : statements) {
                stmt.executeUpdate(statement);
            }
            con.commit();
        } catch (SQLException e) {
            try {
                con.rollback();
            } catch (SQLException e1) {
                // ignore
            }
            throw e;
        } finally {
            closeStatement(stmt);
            closeConnection(con);
        }
    }

    /**
     * Method to execute a query that has a single row as result (for queries
     * with multiple rows as a result, only the first row is returned).
     * 
     * @param query
     *            Query to execute
     * @return List containing the values of the single row
     * @throws SQLException
     */
    public static List<String> executeSingletonQuery(String query) throws SQLException {
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = getConnection();
            stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            if (!rs.next()) {
                return Collections.emptyList();
            } else {
                List<String> result = new ArrayList<String>();
                for (int colIdx = 1; colIdx <= rs.getMetaData().getColumnCount(); colIdx++) {
                    result.add(rs.getString(colIdx));
                }
                return result;
            }
        } catch (SQLException e) {
            try {
                con.rollback();
            } catch (SQLException e1) {
                // ignore
            }
            throw e;
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
            closeConnection(con);
        }
    }

    /**
     * Closes Statement object, ignores SQLExceptions when closing.
     * 
     * @param stmt
     *            Statement (null allowed)
     */
    public static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Closes Connection object, ignores SQLExceptions when closing.
     * 
     * @param stmt
     *            Connection (null allowed)
     */
    public static void closeConnection(Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Closes ResultSet object, ignores SQLExceptions when closing.
     * 
     * @param stmt
     *            ResultSet (null allowed)
     */
    public static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

}
