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
 * Factory to create the right DatabaseInfo object based on supplied
 * configuration information.
 * 
 * @author Mark Rotteveel
 */
public class DatabaseFactory {

    public static DatabaseInfo createDatabaseInfo(SqlSetting settings) throws DatabaseConfigurationException {
        try {
            String brandName = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_BRAND);
            DatabaseSupport dbSupport;
            try {
                dbSupport = DatabaseSupport.valueOf(brandName.toUpperCase());
            } catch (IllegalArgumentException ex) {
                SqlLogger.getLogger().warning("Unknown brandname, defaulting to GENERIC support: " + brandName);
                dbSupport = DatabaseSupport.GENERIC;
            }
            if (!dbSupport.validateSettings(settings)) {
                throw new DatabaseConfigurationException("Provided configuration is incomplete");
            }

            return dbSupport.buildDatabaseInfo(settings);
        } catch (DatabaseConfigurationException ex) {
            throw ex;
        } catch (Exception ex) {
            DatabaseConfigurationException newEx = new DatabaseConfigurationException(ex.getMessage());
            newEx.initCause(ex);
            throw newEx;
        }
    }
}
