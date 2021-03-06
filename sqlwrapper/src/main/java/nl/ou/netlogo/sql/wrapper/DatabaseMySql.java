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
import org.nlogo.api.LogoList;

import java.util.logging.Logger;

/**
 * Implements the MySql specific methods that the SQL extension needs to
 * interact with a MySql database engine
 * 
 * @author NetLogo project-team
 * 
 */
public class DatabaseMySql extends GenericDatabase {

    public static final String BRANDNAME = "MySql";

    private static final Logger LOG = SqlLogger.getLogger();

    public DatabaseMySql(String jdbcUrl, String driverClass, String user, String password,
            boolean autoDisconnect) {
        super(BRANDNAME, jdbcUrl, driverClass, user, password, autoDisconnect);
    }

    @Override
    public void useDatabase(SqlConnection sqlc, String schemaName) throws ExtensionException {
        try {
            SqlStatement statement = sqlc.createStatement("use " + schemaName);
            // the use <database> will never generate a result set
            // nor a row count, hence no further processing.
            statement.executeDirect();
        } catch (Exception e) {
            throw new ExtensionException("Could not switch database context to '" + schemaName + "' " + e);
        }
    }

    @Override
    public boolean findDatabase(SqlConnection sqlc, String schemaName) {
        if (sqlc != null) {
            try {
                SqlStatement statement = sqlc
                        .createStatement("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '"
                                + schemaName + "'");
                if (statement.executeDirect()) {
                    // We have a result, process it.
                    SqlResultSet rs = sqlc.getResultSet();
                    LogoList result = rs.fetchRow();
                    String dbName = (String) result.first();
                    return dbName.equalsIgnoreCase(schemaName);
                }
            } catch (Exception e) {
                // log, but ignore, semantics is: database not found
                LOG.severe("Exception while finding database '" + schemaName + "': " + e);
            }
        }
        return false;
    }

}
