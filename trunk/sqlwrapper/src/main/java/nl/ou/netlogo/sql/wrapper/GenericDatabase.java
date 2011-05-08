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
 * DatabaseInfo implementation for generic database support.
 * 
 * @author Mark Rotteveel
 */
public class GenericDatabase implements DatabaseInfo {

    private final String brandName;
    private final String jdbcUrl;
    private final String driverClass;
    private final String user;
    private final String password;
    private final boolean autoDisconnect;

    /**
     * Constructs a GenericDatabase object.
     * 
     * @param brandName
     *            Brand name (purely informational)
     * @param jdbcUrl
     *            JDBC URL for connection
     * @param driverClass
     *            Driver class specific to use for this database
     * @param user
     *            Username
     * @param password
     *            Password
     * @param autoDisconnect
     *            Autodisconnect usage
     */
    public GenericDatabase(String brandName, String jdbcUrl, String driverClass, String user, String password,
            boolean autoDisconnect) {
        this.brandName = brandName;
        this.jdbcUrl = jdbcUrl;
        this.driverClass = driverClass;
        this.user = user;
        this.password = password;
        this.autoDisconnect = autoDisconnect;
    }

    @Override
    public String getBrandName() {
        return brandName;
    }

    @Override
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public String getDriverClass() {
        return driverClass;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public boolean useAutoDisconnect() {
        return autoDisconnect;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Implementation in GenericDatabase is a no-op (does nothing).
     * </p>
     */
    @Override
    public void useDatabase(SqlConnection conn, String schemaName) throws DatabaseFeatureNotImplementedException,
            ExtensionException {
        // no-op
    }

    /**
     * {@inheritDoc}
     * 
     * @return Implementation in GenericDatabase returns <code>default</code>
     *         always.
     */
    @Override
    public String getCurrentDatabase(SqlConnection conn) throws DatabaseFeatureNotImplementedException {
        return "default";
    }

    /**
     * {@inheritDoc}
     * 
     * @return Implementation in GenericDatabase returns <code>false</code>
     *         always.
     */
    @Override
    public boolean findDatabase(SqlConnection sqlc, String schemaName) throws DatabaseFeatureNotImplementedException {
        return false;
    }
}
