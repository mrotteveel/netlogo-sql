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

import java.sql.SQLException;

import nl.ou.netlogo.sql.wrapper.SqlConnection;
import nl.ou.netlogo.sql.wrapper.SqlEnvironment;
import nl.ou.netlogo.sql.wrapper.SqlExtension;
import nl.ou.netlogo.sql.wrapper.SqlStatement;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;

/**
 * ExecDirect implements the sql:exec-direct command
 * 
 * @author NetLogo project-team
 * 
 */
public class ExecDirect extends DefaultCommand {

    private final SqlEnvironment sqlenv = SqlExtension.getSqlEnvironment();

    /**
     * Checks syntax of the sql:exec-direct command.
     * 
     * @return syntax object handle
     */
    public Syntax getSyntax() {
        return Syntax.commandSyntax(new int[] { Syntax.StringType() });
    }

    /**
     * Executes sql:exec-direct command. If there is already an active
     * statement/result set it is silently closed
     * 
     * @param args
     *            args[0] is the SQL statement
     * @param context
     * @throws ExtensionException
     * @throws org.nlogo.api.LogoException
     */
    public void perform(Argument args[], Context context) throws ExtensionException, org.nlogo.api.LogoException {

        // Get the sql connection for this agent. Exception if none available.
        SqlConnection sqlc = sqlenv.getActiveSqlConnection(context, true);

        try {
            SqlStatement statement = sqlc.createStatement(args[0].getString());
            statement.executeDirect();
        } catch (SQLException sqle) {
            throw new ExtensionException(sqle);
        }
    }
}
