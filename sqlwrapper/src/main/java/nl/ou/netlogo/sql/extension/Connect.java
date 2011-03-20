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
package nl.ou.netlogo.sql.extension;

import nl.ou.netlogo.sql.wrapper.SqlConfiguration;
import nl.ou.netlogo.sql.wrapper.SqlConnection;
import nl.ou.netlogo.sql.wrapper.SqlEnvironment;
import nl.ou.netlogo.sql.wrapper.SqlExtension;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;

import nl.ou.netlogo.sql.wrapper.SqlConfigurable;
import nl.ou.netlogo.sql.wrapper.SqlSetting;

/**
 * Class representing the connect command in a NetLogo model from the SQL
 * extension.
 * 
 * @author NetLogo project-team
 * 
 */
public class Connect extends DefaultCommand {

    private final SqlEnvironment sqlenv = SqlExtension.getSqlEnvironment();

    /**
     * Checks syntax of the connect command.
     * 
     * @return syntax object handle
     */
    public Syntax getSyntax() {
        return Syntax.commandSyntax(new int[] { Syntax.TYPE_LIST });
    }

    /**
     * Executes connect command from model context. If there is an active
     * connection it is silently closed.
     * 
     * @param args
     * @param context
     * @throws ExtensionException
     * @throws org.nlogo.api.LogoException
     */
    public void perform(Argument args[], Context context) throws ExtensionException, org.nlogo.api.LogoException {
        /*
         * Get the sql connection, if any, for this agent. We use
         * getSqlConnection() instead of getActiveSqlConnection() as that will
         * throw an exception when there is no connection found. In this case
         * that is likely to happen as the connect command is typically executed
         * once.
         * 
         * If an active is found we remove it first and release the associated
         * resources [i.e. it is closed].
         */
        SqlConnection sqlc = sqlenv.getSqlConnection(context, false);

        class LocalConfiguration implements SqlConfigurable {
            public String host;
            public int port;
            public String user;
            public String password;
            public String schema;
            public String brand;

            public void configure(SqlSetting settings, Context context) throws Exception {
                host = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_HOST);
                port = settings.getInt(SqlConfiguration.DEFAULTCONNECTION_OPT_PORT);
                user = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_USER);
                password = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_PASSWORD);
                schema = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_DATABASE);
                brand = settings.getString(SqlConfiguration.DEFAULTCONNECTION_OPT_BRAND);
            }
        }

        if (sqlc != null) {
            try {
                sqlc.close();
            } catch (Exception e) {
                throw new ExtensionException("Could not close database connection.");
            }
        }
        try {
            /*
             * the mechanism of SqlConfiguration is used to parse the arguments
             * of the connect command by creating a local configurable object,
             * registering it as SqlConfigurable with SqlConfiguration, parsing
             * the arguments, and removing it from SqlConfiguration
             * 
             * WARNING: This only works because NetLogo executes
             * single-threaded!
             */
            final String name = SqlConfiguration.EXPLICITCONNECTION;
            LocalConfiguration config = new LocalConfiguration();
            sqlenv.getConfiguration().addConfigurable(name, config);
            sqlenv.getConfiguration().setConfiguration(name,
                    SqlConfiguration.parseSettingList(name, args[0].getList()), context);
            sqlenv.getConfiguration().removeConfigurable(name, config);

            sqlc = sqlenv.createConnection(context, config.host, config.port, config.user, config.password,
                    config.schema);
        } catch (RuntimeException e) {
            throw new ExtensionException("Could not connect to database, make sure database is up. " + e);
        }
    }
}
