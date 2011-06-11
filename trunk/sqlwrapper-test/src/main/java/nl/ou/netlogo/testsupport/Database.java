package nl.ou.netlogo.testsupport;

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
    
    public abstract String getDriver();
    
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
    
    public String getAutoDisconnect() {
        return ci.getProperty(ConnectionInformation.AUTODISCONNECT_PROPERTY);
    }
    
    public String getBrand() {
        return brand;
    }
}
