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

import nl.ou.netlogo.sql.wrapper.SqlConnection;
import nl.ou.netlogo.sql.wrapper.SqlEnvironment;
import nl.ou.netlogo.sql.wrapper.SqlExtension;
import nl.ou.netlogo.sql.wrapper.SqlStatement;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoList;
import org.nlogo.api.Syntax;
import java.sql.*;

/**
 * Class representing the exec-query command in a NetLogo model from the SQL
 * extension.
 * 
 * @author NetLogo project-team
 * 
 */
public class ExecQuery extends DefaultCommand {

    private final SqlEnvironment sqlenv = SqlExtension.getSqlEnvironment();

    /**
     * Description of the NetLogo syntax of the command.
     * 
     * @return syntax object handle
     */
    public Syntax getSyntax() {
        int[] right = { Syntax.TYPE_STRING, Syntax.TYPE_LIST };
        return Syntax.commandSyntax(right);
    }

    /**
     * Executes parameterized query command from model context.
     * 
     * @param args
     * @param context
     * @throws ExtensionException
     * @throws org.nlogo.api.LogoException
     */
    public void perform(Argument args[], Context context) throws ExtensionException, org.nlogo.api.LogoException {

        // Get the sql connection for this agent. Exception if none available.
        SqlConnection sqlc = sqlenv.getActiveSqlConnection(context, true);

        try {
            String query = args[0].getString();
            LogoList parameters = args[1].getList();
            SqlStatement statement = sqlc.createStatement(query, parameters);
            statement.executeQuery();
        } catch (SQLException e) {
            throw new ExtensionException(e);
        }
    }
}
