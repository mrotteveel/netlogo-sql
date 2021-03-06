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
import nl.ou.netlogo.sql.wrapper.SqlResultSet;

import org.nlogo.api.*;

/**
 * FetchResultSet implements the sql:fetch-resultset reporter.
 * 
 * @author NetLogo project-team
 * 
 */
public class FetchResultSet extends DefaultReporter {

    private final SqlEnvironment sqlenv = SqlExtension.getSqlEnvironment();

    /**
     * Checks syntax of the sql:fetch-resultset reporter.
     * 
     * @return syntax object handle
     */
    public Syntax getSyntax() {
        return Syntax.reporterSyntax(new int[] {}, Syntax.ListType());
    }

    /**
     * Executes the sql:fetch-resultset reporter.
     * <p>
     * Returns the resultset as a LogoList containing rows (as LogoList).
     * Returns an empty list if the resultset is empty, or if there is no
     * resultset or connection.
     * </p>
     * 
     * @param args
     * @param context
     * @throws ExtensionException
     * @throws org.nlogo.api.LogoException
     */
    public Object report(Argument args[], Context context) throws ExtensionException, org.nlogo.api.LogoException {

        SqlConnection sqlc = sqlenv.getSqlConnection(context, false);

        if (sqlc != null) {
            SqlResultSet resultSet = sqlc.getResultSet();
            if (resultSet != null) {
                return resultSet.fetchResultSet();
            }
        }
        return new LogoListBuilder().toLogoList();
    }
}
