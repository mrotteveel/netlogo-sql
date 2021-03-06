package nl.ou.netlogo.testsupport;

import org.apache.commons.lang.StringUtils;

public enum Database {
    MYSQL(ConnectionInformation.MYSQL_PREFIX) {
        @Override
        public String getDriver() {
            return "com.mysql.jdbc.Driver";
        }
        
        @Override
        public String getJdbcUrl() {
            return String.format("jdbc:mysql://%s:%s/%s", getHost(), getPort(), getSchema());
        }
        
        @Override
        public String charValue(String value, int fieldLength) {
            // MySQL returns CHAR unpadded, contrary to SQL standards
            return value;
        }
    },
    POSTGRESQL(ConnectionInformation.POSTGRESQL_PREFIX){
        @Override
        public String getDriver() {
            return "org.postgresql.Driver";
        }
        
        @Override
        public String getJdbcUrl() {
            return String.format("jdbc:postgresql://%s:%s/%s", getHost(), getPort(), getSchema());
        }
    };
    
    private final String brand;
    private final ConnectionInformation ci;
    
    private Database(String brand) {
        this.brand = brand;
        ci = ConnectionInformation.getInstance(brand);
    }
    
    /**
     * 
     * @return The JDBC driver classname for this database.
     */
    public abstract String getDriver();
    
    /**
     * 
     * @return The JDBC url for this database.
     */
    public abstract String getJdbcUrl();
    
    public String getHost() {
        return ci.getProperty(ConnectionInformation.HOST_PROPERTY);
    }
    
    public String getPort() {
        return ci.getProperty(ConnectionInformation.PORT_PROPERTY);
    }
    
    public String getSchema() {
        return ci.getProperty(ConnectionInformation.SCHEMA_PROPERTY);
    }
    
    public String getUsername() {
        return ci.getProperty(ConnectionInformation.USERNAME_PROPERTY);
    }
    
    public String getPassword() {
        return ci.getProperty(ConnectionInformation.PASSWORD_PROPERTY);
    }
    
    public String getBrand() {
        return brand;
    }
    
    /**
     * 
     * @return the full sql:connect command to connect to this database
     */
    public String getConnectCommand() {
        return String.format("sql:connect [[\"brand\" \"%s\"] [\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"]]",
                getBrand(), getHost(), getPort(), getUsername(), getPassword(), getSchema());
    }
    
    /**
     * Returns the full sql:configure "defaultconnection" command to setup connection pooling to this database.
     * <p>
     * The autodisconnect setting is not set, so the default of the plugin is used.
     * </p>
     * 
     * @return the full sql:configure "defaultconnection" command 
     */
    public String getPoolConfigurationCommand() {
        return String.format("sql:configure \"defaultconnection\" [[\"brand\" \"%s\"] [\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"]]",
                getBrand(), getHost(), getPort(), getUsername(), getPassword(), getSchema());
    }
    
    /**
     * Returns the full sql:configure "defaultconnection" command to setup connection pooling to this database with specified autodisconnect.
     * 
     * @param autodisconnect <code>true</code> enable autodisconnect, <code>false</code> disable autodisconnect.
     * @return the full sql:configure "defaultconnection" command
     */
    public String getPoolConfigurationCommand(boolean autodisconnect) {
        return String.format("sql:configure \"defaultconnection\" [[\"brand\" \"%s\"] [\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"] [\"autodisconnect\" \"%s\"]]",
                        getBrand(), getHost(), getPort(), getUsername(), getPassword(), getSchema(), autodisconnect ? "on" : "off");
    }
    
    /**
     * Returns value as the database would retrieve for a CHAR field.
     * <p>
     * Rationale: SQL standards dictates that CHAR fields should be right padded with spaces,
     * unfortunately not all databases (read: MySQL) behave correctly. This method can be used to get the
     * right value for comparison tests.
     * </p>
     * 
     * @param value Value
     * @param fieldLength Length of the CHAR field
     * @return Value as it would be returned from the database
     */
    public String charValue(String value, int fieldLength) {
        return StringUtils.rightPad(value, fieldLength);
    }
}
