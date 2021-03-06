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

import org.nlogo.api.ExtensionException;

/**
 * Defines the methods that must be implemented by a database specific class for
 * interaction with the database engine.
 * 
 * @author NetLogo project-team
 * 
 */
public interface DatabaseInfo {

    /**
     * @return database url for JDBC
     */
    String getJdbcUrl();

    /**
     * @return JDBC driver class for database
     */
    String getDriverClass();

    /**
     * @param conn
     *            valid connection to database engine
     * @param schemaName
     *            name of schema/database to use
     * @throws DatabaseFeatureNotImplementedException
     *             If this feature is not available
     * @throws ExtensionException
     */
    void useDatabase(SqlConnection conn, String schemaName) throws DatabaseFeatureNotImplementedException,
            ExtensionException;

    /**
     * @param conn
     *            valid connection to database engine
     * @return name of schema/database currently in use
     * @throws DatabaseFeatureNotImplementedException
     *             If this feature is not available
     */
    String getCurrentDatabase(SqlConnection conn) throws DatabaseFeatureNotImplementedException;

    /**
     * @param sqlc
     *            valid connection to database engine
     * @param schemaName
     *            name of schema/database to search for
     * @return indication whether named schema could be found
     * @throws DatabaseFeatureNotImplementedException
     *             If this feature is not available
     */
    boolean findDatabase(SqlConnection sqlc, String schemaName) throws DatabaseFeatureNotImplementedException;

    /**
     * @return configured user to access database engine
     */
    String getUser();

    /**
     * @return configured password to access database engine
     */
    String getPassword();

    /**
     * @return name indicating the brand of the database
     */
    String getBrandName();

    /**
     * @return indication whether auto disconnect is on or off
     */
    boolean useAutoDisconnect();
}
