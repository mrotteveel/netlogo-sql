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
        for (Database db : Database.values()) {
            try {
                Class.forName(db.getDriver());
            } catch (ClassNotFoundException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
    }
    
    public static final Database PLUGIN_DEFAULT_DATABASE = Database.MYSQL;

    /**
     * Creates a connection to the specified test database.
     * 
     * @param db
     *            Database to connect to
     * @return Connection to the test database
     * @throws SQLException
     */
    public static Connection getConnection(Database db) throws SQLException {
        return DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword());
    }

    /**
     * Creates the default sql:connect command for a valid connection using the
     * default brand (MySQL).
     * 
     * @return sql:connect command
     */
    public static String getDefaultConnectCommand() {
        Database db = PLUGIN_DEFAULT_DATABASE;
        return String.format("sql:connect [[\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"]]",
                        db.getHost(), db.getPort(), db.getUsername(), db.getPassword(), db.getSchema());
    }

    /**
     * Creates the default sql:connect command for a valid connection using the
     * generic brand.
     * 
     * @return sql:connect command
     */
    public static String getGenericConnectCommand() {
        Database db = PLUGIN_DEFAULT_DATABASE;
        return String.format("sql:connect [[\"brand\" \"generic\"] [\"jdbc-url\" \"%s\"] [\"driver\" \"com.mysql.jdbc.Driver\"] [\"user\" \"%s\"] [\"password\" \"%s\"]]",
                        db.getJdbcUrl(), db.getUsername(), db.getPassword());
    }

    /**
     * Creates the default sql:set-default command for a valid pool
     * configuration using the default brand (MySQL). 
     * <p>
     * Autodisconnect is not explicitly set for this command and uses the plugin default. Use
     * {@link #getDefaultPoolConfigurationCommand(boolean)} for explicit control.
     * </p>
     * 
     * @return sql:set-default "connect" command
     */
    public static String getDefaultPoolConfigurationCommand() {
        Database db = PLUGIN_DEFAULT_DATABASE;
        return String.format("sql:configure \"defaultconnection\" [[\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"]]",
                        db.getHost(), db.getPort().toString(), db.getUsername(), db.getPassword(), db.getSchema());
    }

    /**
     * Creates the default sql:set-default command for a valid pool
     * configuration using the generic brand.
     * <p>
     * Autodisconnect is not explicitly set for this command and uses the plugin default. Use
     * {@link #getGenericPoolConfigurationCommand(boolean)} for explicit
     * control.
     * </p>
     * 
     * @return sql:set-default "connect" command
     */
    public static String getGenericPoolConfigurationCommand() {
        Database db = PLUGIN_DEFAULT_DATABASE;
        return String.format("sql:configure \"defaultconnection\" [[\"brand\" \"generic\"] [\"jdbc-url\" \"%s\"] [\"driver\" \"com.mysql.jdbc.Driver\"] [\"user\" \"%s\"] [\"password\" \"%s\"]]",
                        db.getJdbcUrl(), db.getUsername(), db.getPassword());
    }

    /**
     * Creates the default sql:set-default command for a valid pool
     * configuration using the default brand (MySQL), and allows for switching
     * autodisconnect on/off.
     * 
     * @return sql:set-default "connect" command
     */
    public static String getDefaultPoolConfigurationCommand(boolean autodisconnect) {
        Database db = PLUGIN_DEFAULT_DATABASE;
        return String.format("sql:configure \"defaultconnection\" [[\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"] [\"autodisconnect\" \"%s\"]]",
                        db.getHost(), db.getPort().toString(), db.getUsername(), db.getPassword(), db.getSchema(),
                        autodisconnect ? "on" : "off");
    }

    /**
     * Creates the default sql:set-default command for a valid pool
     * configuration using the generic brand, and allows for switching
     * autodisconnect on/off.
     * 
     * @return sql:set-default "connect" command
     */
    public static String getGenericPoolConfigurationCommand(boolean autodisconnect) {
        Database db = PLUGIN_DEFAULT_DATABASE;
        return String.format("sql:configure \"defaultconnection\" [[\"brand\" \"generic\"] [\"jdbc-url\" \"%s\"] [\"driver\" \"com.mysql.jdbc.Driver\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"autodisconnect\" \"%s\"]]",
                        db.getJdbcUrl(), db.getUsername(), db.getPassword(), autodisconnect ? "on" : "off");
    }

    /**
     * Method to execute DDL or DML (but not SELECT) statements on the MySQL
     * test database.
     * <p>
     * These statements are executed against the default test database (MySQL).
     * </p>
     * 
     * @param statements
     *            Statements to execute
     * @throws SQLException
     *             For any error executing the statements.
     */
    public static void executeUpdate(String... statements) throws SQLException {
        executeUpdate(PLUGIN_DEFAULT_DATABASE, statements);
    }

    /**
     * Method to execute DDL or DML (but not SELECT) statements.
     * 
     * @param db
     *            Database to use for these queries
     * @param statements
     *            Statements to execute
     * @throws SQLException
     *             For any error executing the statements.
     */
    public static void executeUpdate(Database db, String... statements) throws SQLException {
        Connection con = null;
        Statement stmt = null;
        try {
            con = getConnection(db);
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
     * <p>
     * This query is executed against the default test database (MySQL)
     * </p>
     * 
     * @param query
     *            Query to execute
     * @return List containing the values of the single row
     * @throws SQLException
     */
    public static List<String> executeSingletonQuery(String query) throws SQLException {
        return executeSingletonQuery(PLUGIN_DEFAULT_DATABASE, query);
    }

    /**
     * Method to execute a query that has a single row as result (for queries
     * with multiple rows as a result, only the first row is returned).
     * 
     * @param db
     *            Database to use for this query
     * @param query
     *            Query to execute
     * @return List containing the values of the single row
     * @throws SQLException
     */
    public static List<String> executeSingletonQuery(Database db, String query) throws SQLException {
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = getConnection(db);
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
