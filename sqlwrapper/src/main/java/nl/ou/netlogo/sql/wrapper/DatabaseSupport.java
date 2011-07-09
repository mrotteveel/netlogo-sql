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

/**
 * Provides explicit support for databases
 * 
 * @author Mark Rotteveel
 */
public enum DatabaseSupport {

    GENERIC {
        @Override
        public String buildJdbcUrl(SqlSetting settings) throws Exception {
            return settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_JDBC_URL);
        }
        
        @Override
        protected String getDefaultDriverClass() {
            return SqlSetting.DEFAULT_UNSET;
        }

        @Override
        public boolean validateSettings(SqlSetting settings) throws Exception {
            if (!settings.isValid()) {
                return false;
            }
            // Generic database requires JDBC URL
            if (settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_JDBC_URL).equals(SqlSetting.DEFAULT_UNSET)) {
                return false;
            }
            // Generic database requires driver classname
            if (settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DRIVER_CLASSNAME).equals(
                    SqlSetting.DEFAULT_UNSET)) {
                return false;
            }
            return true;
        }

        @Override
        protected DatabaseInfo buildDatabaseInfo(String brandName, String jdbcUrl, String driverClass, String user,
                String password, boolean autoDisconnect) throws Exception {
            return new GenericDatabase(name(), jdbcUrl, driverClass, user, password, autoDisconnect);
        }
    },
    MYSQL {
        @Override
        public String buildJdbcUrl(SqlSetting settings) throws Exception {
            final String jdbcPattern = "jdbc:mysql://%s:%d/%s";
            final int defaultPort = 3306;
            String host = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_HOST);
            int port = settings.getInt(SqlConfiguration.DEFAULTCONNECTION_OPT_PORT);
            String database = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DATABASE);

            port = port != 0 ? port : defaultPort;
            return String.format(jdbcPattern, host, port, database);
        }
        
        @Override
        protected String getDefaultDriverClass() {
            return "com.mysql.jdbc.Driver";
        }

        @Override
        public boolean validateSettings(SqlSetting settings) throws Exception {
            if (!settings.isValid()) {
                return false;
            }
            // Name of the database schema is required for MySQL
            if (settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DATABASE).equals(SqlSetting.DEFAULT_UNSET)) {
                return false;
            }
            return true;
        }

        @Override
        protected DatabaseInfo buildDatabaseInfo(String brandName, String jdbcUrl, String driverClass, String user,
                String password, boolean autoDisconnect) throws Exception {
            return new DatabaseMySql(jdbcUrl, driverClass, user, password, autoDisconnect);
        }
    },
    POSTGRESQL {

        @Override
        public String buildJdbcUrl(SqlSetting settings) throws Exception {
            final String jdbcPattern = "jdbc:postgresql://%s:%d/%s";
            final int defaultPort = 5432;
            String host = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_HOST);
            int port = settings.getInt(SqlConfiguration.DEFAULTCONNECTION_OPT_PORT);
            String database = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DATABASE);

            port = port != 0 ? port : defaultPort;
            return String.format(jdbcPattern, host, port, database);
        }
        
        @Override
        protected String getDefaultDriverClass() {
            return "org.postgresql.Driver";
        }

        @Override
        public boolean validateSettings(SqlSetting settings) throws Exception {
            if (!settings.isValid()) {
                return false;
            }
            // Name of the database schema is required for PostgreSQL
            if (settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DATABASE).equals(SqlSetting.DEFAULT_UNSET)) {
                return false;
            }
            return true;
        }

        @Override
        protected DatabaseInfo buildDatabaseInfo(String brandName, String jdbcUrl, String driverClass, String user,
                String password, boolean autoDisconnect) throws Exception {
            return new GenericDatabase(name(), jdbcUrl, driverClass, user, password, autoDisconnect);
        }
    };

    /**
     * Builds the JDBC URL for this database based on the provided settings.
     * 
     * @param settings
     *            SqlSetting object
     * @return JDBC URL
     * @throws Exception
     */
    public abstract String buildJdbcUrl(SqlSetting settings) throws Exception;

    /**
     * Returns the driver class for this database based on the provided
     * settings.
     * 
     * @param settings
     *            SqlSetting object
     * @return Name of the JDBC driver class
     * @throws Exception
     */
    public final String getDriverClass(SqlSetting settings) throws Exception {
        String driverClass = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DRIVER_CLASSNAME);
        if (driverClass.equals(SqlSetting.DEFAULT_UNSET)) {
            return getDefaultDriverClass();
        }
        return driverClass;
    }
    
    /**
     * Returns the default driver class for this database.
     * 
     * @return Name of the default JDBC driver class, or {@link SqlSetting#DEFAULT_UNSET} if there is no default
     */
    protected abstract String getDefaultDriverClass();

    /**
     * Provides database specific validation of the settings object.
     * 
     * @param settings
     *            SqlSetting object
     * @return <code>true</code> if settings are valid to configure database,
     *         false otherwise
     */
    public abstract boolean validateSettings(SqlSetting settings) throws Exception;

    /**
     * Build the DatabaseInfo based on the settings.
     * 
     * @param settings
     *            SqlSetting object
     * @return DatabaseInfo object based on the supplied settings
     * @throws Exception
     */
    public DatabaseInfo buildDatabaseInfo(SqlSetting settings) throws Exception {
        boolean autoDisconnect = false;
        // Autodisconnect should only apply to the default connection (connection pool)
        if (settings.getName().equals(SqlConfiguration.DEFAULTCONNECTION)) {
            autoDisconnect = SqlSetting.toggleValue(settings
                    .getString(SqlConfiguration.DEFAULTCONNECTION_OPT_AUTODISCONNECT));
        }
        return buildDatabaseInfo(name(), buildJdbcUrl(settings), getDriverClass(settings),
                settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_USER),
                settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_PASSWORD), autoDisconnect);
    }

    /**
     * Builds the DatabaseInfo based on the provided parameters. Used by default
     * implementation of {@link #buildDatabaseInfo(SqlSetting)}
     * 
     * @param brandName
     *            Brandname of the database
     * @param jdbcUrl
     *            JDBC URL to the database
     * @param driverClass
     *            JDBC driver class
     * @param user
     *            User
     * @param password
     *            Password
     * @param autoDisconnect
     *            Use autodisconnect
     * @return DatabaseInfo object based on the supplied parameters
     * @throws Exception
     */
    protected abstract DatabaseInfo buildDatabaseInfo(String brandName, String jdbcUrl, String driverClass,
            String user, String password, boolean autoDisconnect) throws Exception;
}
