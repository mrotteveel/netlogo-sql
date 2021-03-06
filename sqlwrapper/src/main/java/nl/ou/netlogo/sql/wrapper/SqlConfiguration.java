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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.nlogo.api.Context;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoList;

/**
 * Manages the configurable objects in the SQL extension. Acts as an interface
 * between the sql:configure command, and the objects affected by that command.
 * Sets defaults for all aspects and options whenever a valid default setting
 * makes sense, a special invalid default setting when not.
 * 
 * @see SqlConfigurable
 * @see SqlSetting
 * 
 * @author NetLogo project-team
 * 
 */
public class SqlConfiguration {

    //
    // configurable aspects
    //
    public static final String DEFAULTCONNECTION = "defaultconnection";
    public static final String CONNECTIONPOOL = "connectionpool";
    public static final String LOGGING = "logging";
    public static final String EXPLICITCONNECTION = "explicit-connection";

    //
    // configurable options for above aspects, the strings are options for the
    // sql:configure command
    //
    public static final String DEFAULTCONNECTION_OPT_BRAND = "brand";
    public static final String DEFAULTCONNECTION_OPT_HOST = "host";
    public static final String DEFAULTCONNECTION_OPT_PORT = "port";
    public static final String DEFAULTCONNECTION_OPT_USER = "user";
    public static final String DEFAULTCONNECTION_OPT_PASSWORD = "password";
    public static final String DEFAULTCONNECTION_OPT_DATABASE = "database";
    public static final String DEFAULTCONNECTION_OPT_AUTODISCONNECT = "autodisconnect";
    public static final String DEFAULTCONNECTION_OPT_DRIVER_CLASSNAME = "driver";
    public static final String DEFAULTCONNECTION_OPT_JDBC_URL = "jdbc-url";
    public static final String CONNECTIONPOOL_OPT_PARTITIONS = "partitions";
    public static final String CONNECTIONPOOL_OPT_MAXCONNECTIONS = "max-connections";
    public static final String CONNECTIONPOOL_OPT_TIMEOUT = "timeout";
    public static final String LOGGING_OPT_PATH = "path";
    public static final String LOGGING_OPT_LOGGING = "file-logging";
    public static final String LOGGING_OPT_LEVEL = "level";
    public static final String LOGGING_OPT_COPYTOSTDERR = "copy-to-stderr";

    /**
     * available contains all available aspects
     */
    private Map<String, SqlSetting> available = new HashMap<String, SqlSetting>();

    /**
     * configured contains all aspects that have been configured, either at
     * initialization time of the extension, or through a call of sql:configure
     */
    private Map<String, SqlSetting> configured = new HashMap<String, SqlSetting>();

    /*
     * configurables contains all configurable options for all aspects, with
     * their defaults
     */
    private Map<String, Set<SqlConfigurable>> configurables = new HashMap<String, Set<SqlConfigurable>>();

    /**
     * Default constructor, initializes all available aspects and options
     */
    public SqlConfiguration() {
        /*
         * For each configurable aspect, there is a set of default settings. If
         * there is no valid default setting for an aspect, the default value
         * used must be SqlSetting.DefaultInvalidSetting, as this will allow
         * invalid/incomplete configurations to be caught before trying to use
         * them
         */
        String[][] defaultConnectionSettings = { 
                { DEFAULTCONNECTION_OPT_BRAND, "MySql" },
                { DEFAULTCONNECTION_OPT_HOST, "localhost" },
                { DEFAULTCONNECTION_OPT_PORT, SqlSetting.DEFAULT_UNSET },
                { DEFAULTCONNECTION_OPT_DATABASE, SqlSetting.DEFAULT_UNSET },
                { DEFAULTCONNECTION_OPT_USER, SqlSetting.DEFAULT_INVALID },
                { DEFAULTCONNECTION_OPT_PASSWORD, SqlSetting.DEFAULT_INVALID },
                { DEFAULTCONNECTION_OPT_JDBC_URL, SqlSetting.DEFAULT_UNSET },
                { DEFAULTCONNECTION_OPT_DRIVER_CLASSNAME, SqlSetting.DEFAULT_UNSET },
                { DEFAULTCONNECTION_OPT_AUTODISCONNECT, "on" },
        };
        //
        // connectSettings is used for explicit connections done
        // through the sql:connect command. This set is then used to validate
        // the arguments for the sql:command. For ease of use and consistency,
        // this set should be a subset of defaultConnectionSettings
        //
        String[][] connectSettings = {
        		{ DEFAULTCONNECTION_OPT_BRAND, "MySql" },
                { DEFAULTCONNECTION_OPT_HOST, "localhost" },
                { DEFAULTCONNECTION_OPT_PORT, SqlSetting.DEFAULT_UNSET },
                { DEFAULTCONNECTION_OPT_DATABASE, SqlSetting.DEFAULT_UNSET },
                { DEFAULTCONNECTION_OPT_USER, SqlSetting.DEFAULT_INVALID },
                { DEFAULTCONNECTION_OPT_PASSWORD, SqlSetting.DEFAULT_INVALID },
                { DEFAULTCONNECTION_OPT_JDBC_URL, SqlSetting.DEFAULT_UNSET },
                { DEFAULTCONNECTION_OPT_DRIVER_CLASSNAME, SqlSetting.DEFAULT_UNSET },
        };
        String[][] connectionPoolSettings = { 
        		{ CONNECTIONPOOL_OPT_PARTITIONS, "1" },
                { CONNECTIONPOOL_OPT_MAXCONNECTIONS, "20" },
                { CONNECTIONPOOL_OPT_TIMEOUT, "5" },
        };
        /*
         * NB: the FileHandler for logging is disfunctional for reasons so far
         * unknown/not understood if you need logging, turn COPYTOSTDERR on.
         * Switching on the FileHandler does not actually send any logging to a
         * file
         */
        String[][] loggingSettings = {
        		{ LOGGING_OPT_PATH, "%t" },
        		{ LOGGING_OPT_LOGGING, "off" },
                { LOGGING_OPT_LEVEL, "ALL" },
                { LOGGING_OPT_COPYTOSTDERR, "off" },
        };
        try {
            addAvailable(DEFAULTCONNECTION, defaultConnectionSettings);
            addAvailable(CONNECTIONPOOL, connectionPoolSettings);
            addAvailable(LOGGING, loggingSettings);
            addAvailable(EXPLICITCONNECTION, connectSettings);
        } catch (Exception ex) {
            // ignore, for now. should log error at least
            System.err.println("SqlConfiguration: broke in constructor\n");
            ex.printStackTrace();
        }
    }

    /**
     * Adds an configurable aspect to the available aspects
     * 
     * @param name
     *            name of aspect, should match a key in available and optional
     *            configured
     * @param settings
     *            array of name value pairs: "option-name" => "default-value"
     * @throws ExtensionException
     */
    private void addAvailable(String name, String[][] settings) throws ExtensionException {
        if (!available.containsKey(name)) {
            try {
                available.put(name, new SqlSetting(name, settings));
            } catch (Exception ex) {
                throw new ExtensionException(ex);
            }
        }
    }

    /**
     * Implementation of the sql:configure command
     * 
     * @param name
     *            aspect to configure
     * @param configurable
     *            object that implements the SqlConfigurable interface
     * @throws ExtensionException
     * 
     * @see SqlConfigurable
     */
    public void addConfigurable(String name, SqlConfigurable configurable) throws ExtensionException {
        SqlLogger.getLogger().fine("Adding configuration for '" + name + "'");
        if (available.containsKey(name)) {
            if (!configurables.containsKey(name)) {
                configurables.put(name, new HashSet<SqlConfigurable>());
            }
            configurables.get(name).add(configurable);
            try {
                configurable.configure(getConfiguration(name), null);
            } catch (Exception ex) {
                throw new ExtensionException("Problem while configuring '" + name + "' (" + configurable + "): " + ex);
            }
        }
    }

    /**
     * Removes a configurable object from the list of configurable objects
     * 
     * @param name
     *            name of aspect
     * @param configurable
     *            object that was previously added through addConfigurable()
     * 
     * @see #addConfigurable(String name, SqlConfigurable configurable)
     */
    public void removeConfigurable(String name, SqlConfigurable configurable) {
        if (!configurables.containsKey(name)) {
            configurables.get(name).remove(configurable);
        }
    }

    /**
     * Implementation of the sql:configure command
     * 
     * @param name
     *            name of aspect to configure
     * @param keyValuePairs
     *            Map of key => value pairs of configurable options with values
     * @param context
     *            Context object identifying the context the sql:configure was
     *            called from
     * @throws ExtensionException
     */
    public void setConfiguration(String name, Map<String, String> keyValuePairs, Context context)
            throws ExtensionException {
        SqlSetting availableSetting = available.get(name);
        SqlSetting configuredSetting;
        if (availableSetting != null && availableSetting.isVisible()) {
            if (configured.get(name) == null) {
                // start with a copy of the available setting, this will set defaults
                try {
                    availableSetting = available.get(name);
                    SqlSetting newSetting = available.get(name).clone();
                    configured.put(name, newSetting);
                } catch (Exception ex) {
                    SqlLogger.getLogger().severe("Problem setting '" + name + "'");
                    throw new ExtensionException("setConfiguration('" + name
                            + "'): while copying from available settings: " + ex);
                }
            }
            configuredSetting = configured.get(name);
            configuredSetting.assignSettings(keyValuePairs, availableSetting);
        } else {
            String message = "Attempt to configure for unknown name ('" + name + "')";
            SqlLogger.getLogger().severe(message);
            throw new ExtensionException(message);
        }

        // push the new settings to any registered SqlConfigurable
        if (configurables.containsKey(name)) {
            for (SqlConfigurable configurable : configurables.get(name)) {
                try {
                    configurable.configure(configuredSetting, context);
                } catch (Exception ex) {
                    String message = "setConfiguration('" + name + "'): while calling configurable.configure: " + ex;
                    SqlLogger.getLogger().severe(message);
                    throw new ExtensionException(message);
                }
            }
        }
    }

    /**
     * Retrieves a configuration, from the configured set if available,
     * otherwise from the available set.
     * 
     * @param name
     *            name of aspect
     * @return SqlSetting
     * @throws ExtensionException
     */
    public SqlSetting getConfiguration(String name) throws ExtensionException {
        if (configured.containsKey(name)) {
            return configured.get(name);
        } else if (available.containsKey(name)) {
            try {
                return available.get(name).clone();
            } catch (CloneNotSupportedException e) {
                String message = "Unable to clone SqlSetting object for name '" + name + "'";
                SqlLogger.getLogger().severe(message);
                throw new ExtensionException(message);
            }
        } else {
            String message = "Attempt to get configuration for unknown name ('" + name + "')";
            SqlLogger.getLogger().severe(message);
            throw new ExtensionException(message);
        }
    }

    /**
     * Retrieves a set of all names of aspects available for configuration
     * 
     * @return set of names
     */
    public Set<String> keySet() {
        return available.keySet();
    }

    /**
     * Retrieves a map of all configured aspects
     * 
     * @return Map<String, SqlSetting>
     */
    public Map<String, SqlSetting> getConfiguration() {
        return configured;
    }

    /**
     * Checks a list of settings in the form of a LogoList, and returns a Map of
     * key => value pairs if the list seems valid
     * 
     * @param name
     *            name of aspect
     * @param settingList
     *            LogoList of key => value pairs
     * @return Map of key => value pairs
     * @throws ExtensionException
     */
    public static Map<String, String> parseSettingList(String name, LogoList settingList) throws ExtensionException {
        Map<String, String> kvPairs = new HashMap<String, String>();
        for (Object obj : settingList) {
            LogoList logoKvPair = (LogoList) obj;
            /*
             * a key-value pair could be valid if
             * - it is indeed a pair of strings, or
             * - there is only one string, and it is the only element in the settingList,
             *   in which case it is the single value for a single setting aspect
             */
            try {
                if (logoKvPair.size() == 2) {
                    kvPairs.put(logoKvPair.get(0).toString(), logoKvPair.get(1).toString());
                } else if (logoKvPair.size() == 1 && settingList.size() == 1) {
                    kvPairs.put("", logoKvPair.get(0).toString());
                } else {
                    throw new ExtensionException("Wrong number of arguments in list");
                }
            } catch (Exception ex) {
                throw new ExtensionException("sql:configure: invalid [key value] pair for name '" + name + "' (" + ex
                        + ")");
            }
        }

        return kvPairs;
    }
}
