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
 * Provides explit support for databases
 * 
 * @author Mark Rotteveel
 */
public enum DatabaseSupport {
    
    GENERIC {
        public String buildJdbcUrl(SqlSetting settings) throws Exception {
            return settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_JDBC_URL);
        }
        
        public String getDriverClass(SqlSetting settings) throws Exception {
            return settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DRIVER_CLASSNAME);
        }
        
        public boolean validateSettings(SqlSetting settings) throws Exception {
            if (!settings.isValid()) {
                return false;
            }
            if (settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_JDBC_URL).equals(SqlSetting.DEFAULT_UNSET)) {
                return false;
            }
            if (settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DRIVER_CLASSNAME).equals(SqlSetting.DEFAULT_UNSET)) {
                return false;
            }
            return true;
        }
        
        public DatabaseInfo buildDatabaseInfo(SqlSetting settings) throws Exception {
            boolean autoDisconnect = false;
            if (settings.getName().equals(SqlConfiguration.DEFAULTCONNECTION)) {
                autoDisconnect = SqlSetting.toggleValue(settings
                        .getString(SqlConfiguration.DEFAULTCONNECTION_OPT_AUTODISCONNECT));
            }
            return new GenericDatabase(name(), buildJdbcUrl(settings), getDriverClass(settings),
                    settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_USER),
                    settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_PASSWORD), autoDisconnect);
        }
    },
    MYSQL {
        public String buildJdbcUrl(SqlSetting settings) throws Exception {
            final String jdbcPattern = "jdbc:mysql://%s:%d/%s";
            final int defaultPort = 3306;
            String host = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_HOST);
            int port = settings.getInt(SqlConfiguration.DEFAULTCONNECTION_OPT_PORT);
            String database = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DATABASE);
            
            port = port != 0 ? port : defaultPort;
            return String.format(jdbcPattern, host, port, database);
        }
        
        public String getDriverClass(SqlSetting settings) throws Exception {
            String driverClass = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DRIVER_CLASSNAME);
            if (driverClass.equals(SqlSetting.DEFAULT_UNSET)) {
                return "com.mysql.jdbc.Driver";
            }
            return driverClass;
        }
        
        public boolean validateSettings(SqlSetting settings) throws Exception {
            if (!settings.isValid()) {
                return false;
            }
            if (settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DATABASE).equals(SqlSetting.DEFAULT_UNSET)) {
                return false;
            }
            return true;
        }
        
        public DatabaseInfo buildDatabaseInfo(SqlSetting settings) throws Exception {
            boolean autoDisconnect = false;
            if (settings.getName().equals(SqlConfiguration.DEFAULTCONNECTION)) {
                autoDisconnect = SqlSetting.toggleValue(settings
                        .getString(SqlConfiguration.DEFAULTCONNECTION_OPT_AUTODISCONNECT));
            }
            return new DatabaseMySql(buildJdbcUrl(settings), getDriverClass(settings),
                    settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_USER),
                    settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_PASSWORD), autoDisconnect);
        }
    };
    
    /**
     * Builds the JDBC URL for this database based on the provided settings.
     * 
     * @param settings SqlSetting object
     * @return JDBC URL
     * @throws Exception
     */
    public abstract String buildJdbcUrl(SqlSetting settings) throws Exception;
    
    /**
     * Returns the driver class for this database based on the provided settings.
     * 
     * @param settings SqlSetting object
     * @return Name of the JDBC driver class
     * @throws Exception
     */
    public abstract String getDriverClass(SqlSetting settings) throws Exception;
    
    /**
     * Provides database specific validation of the settings object.
     * 
     * @param settings SqlSetting object
     * @return <code>true</code> if settings are valid to configure database, false otherwise
     */
    public abstract boolean validateSettings(SqlSetting settings) throws Exception;
    
    public abstract DatabaseInfo buildDatabaseInfo(SqlSetting settings) throws Exception;
}
