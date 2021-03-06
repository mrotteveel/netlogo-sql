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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.nlogo.api.ExtensionException;

public class SqlSetting implements Cloneable {
    private String name;
    private boolean visible;
    private HashMap<String, String> settings = new HashMap<String, String>();
    /**
     * Marker String for a setting that by default has an invalid value (and is thus required to be configured)
     */
    public static final String DEFAULT_INVALID = "<default, invalid>";
    /**
     * Marker String for a setting that by default is unset.
     */
    public static final String DEFAULT_UNSET = "<default>";

    public static final Logger LOG = SqlLogger.getLogger();

    protected SqlSetting() {
    }

    public SqlSetting(String name, String[][] settings) throws Exception {
        this.name = name;
        for (int i = 0; i < settings.length; ++i) {
            if (settings[i].length == 2) {
                this.settings.put(settings[i][0], settings[i][1]);
            } else {
                throw new Exception("Wrong number of elements in settings[" + i + "] for '" + name + "'");
            }
        }
        this.visible = true;
    }
    
    /**
     * Assigns the key/value pairs to a SqlSetting object
     * 
     * @param keyValuePairs Map containing the settings
     * @param settingConstraint Constraint for settings (keys must be available in this setting), optional: use null to not check settings
     * @throws ExtensionException For errors assigning values (eg setting-key not available in settingConstraint)
     */
    public void assignSettings(Map<String, String> keyValuePairs, SqlSetting settingConstraint) throws ExtensionException {
        for (Map.Entry<String, String> entry : keyValuePairs.entrySet()) {
            if (settingConstraint == null || settingConstraint.containsKey(entry.getKey())) {
                settings.put(entry.getKey(), entry.getValue());
            } else {
                String message = "Attempt to configure unknown key '" + entry.getKey();
                SqlLogger.getLogger().severe(message);
                throw new ExtensionException(message);
            }
        }
    }

    /**
     * Retrieves the single configuration value as a String.
     * <p>
     * Only works for SqlSettings with only one setting.
     * </p>
     * 
     * @return Setting value
     * @throws Exception
     *             If this SqlSetting contains more than one setting.
     */
    public String getString() throws Exception {
        // only valid if there is only one setting
        if (settings.size() == 1) {
            Set<String> keys = settings.keySet();
            Iterator<String> it = keys.iterator();
            return settings.get(it.next());
        } else {
            throw new Exception(
                    "Attempt to read single setting from SqlSetting class containing 0 or multiple settings (name: '"
                            + name + "'");
        }
    }

    /**
     * Retrieves the setting value as a String for the specified key.
     * 
     * @param key
     *            Name of the setting
     * @return Setting value
     * @throws Exception
     *             If the specified setting key does not exist
     */
    public String getString(String key) throws Exception {
        if (settings.containsKey(key)) {
            return settings.get(key);
        } else {
            throw new Exception("Attempt to read setting for non-existent key '" + key + "' for name '" + name + "'");
        }
    }

    /**
     * Retrieves the setting value as an integer for the specified key.
     * 
     * @param key
     *            Name of the setting
     * @return Setting value as integer
     * @throws Exception
     *             If the specified setting key does not exist, or the value
     *             cannot be converted to an integer.
     */
    public int getInt(String key) throws Exception {
        if (settings.containsKey(key)) {
            String value = settings.get(key);
            if (DEFAULT_UNSET.equals(value)) {
                return 0;
            }
            return (int) Double.parseDouble(value);
        } else {
            throw new Exception("Attempt to read setting for non-existent key '" + key + "' for name '" + name + "'");
        }
    }

    /**
     * Retrieves the setting value as a long for the specified key.
     * 
     * @param key
     *            Name of the setting
     * @return Setting value as long
     * @throws Exception
     *             If the specified setting key does not exist, or the value
     *             cannot be converted to a long.
     */
    public long getLong(String key) throws Exception {
        if (settings.containsKey(key)) {
            return (long) Double.parseDouble(settings.get(key));
        } else {
            throw new Exception("Attempt to read setting for non-existent key '" + key + "' for name '" + name + "'");
        }
    }

    public void put(String key, String value) {
        settings.put(key, value);
    }

    public boolean containsKey(String key) {
        return settings.containsKey(key);
    }

    public Set<String> keySet() {
        return settings.keySet();
    }

    public String getName() {
        return name;
    }

    public boolean isValid() {
        for (String value : settings.values()) {
            if (value.equals(SqlSetting.DEFAULT_INVALID)) {
                return false;
            }
        }

        return true;
    }

    public boolean isVisible() {
        return this.visible;
    }

    @SuppressWarnings("unchecked")
    @Override
    public SqlSetting clone() throws CloneNotSupportedException {
        SqlSetting clone = (SqlSetting) super.clone();
        clone.settings = (HashMap<String, String>) settings.clone();
        return clone;
    }

    /**
     * Interprets the value of a toggle string (on/off/true/false, case
     * insensitive) Throws an exception when toggle does not match a valid
     * string
     * 
     * @param toggle
     *            string, should match "on", "off", "true" or "false", case
     *            insensitive
     * @return boolean interpretation of toggle
     * @throws Exception
     *             when toggle contains invalid string
     */
    public static boolean toggleValue(String toggle) throws Exception {
        return interpretToggle(toggle);
    }

    /**
     * Resilient version of toggleValue(), returns previousValue when toggle is
     * invalid
     * 
     * @param toggle
     *            string, should match "on", "off", "true" or "false", case
     *            insensitive
     * @param previousValue
     *            value to return when toggle is invalid
     * @return boolean interpretation of toggle
     */
    public static boolean toggleValue(String toggle, boolean previousValue) {
        try {
            return interpretToggle(toggle);
        } catch (Exception ex) {
            return previousValue;
        }
    }

    /**
     * Interprets value of toggle string as boolean
     * 
     * @param toggle
     *            string, should match "on", "off", "true" or "false", case
     *            insensitive
     * @return boolean interpretation of toggle
     * @throws Exception
     *             Exception when toggle contains invalid string
     */
    private static boolean interpretToggle(String toggle) throws Exception {
        if (toggle.equalsIgnoreCase("on") || toggle.equalsIgnoreCase("true")) {
            return true;
        } else if (toggle.equalsIgnoreCase("off") || toggle.equalsIgnoreCase("false")) {
            return false;
        }

        throw new Exception("Invalid toggle value: '" + toggle + "'");
    }
}
