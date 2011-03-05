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

import org.nlogo.api.*;


/**
 * GetRowCount implements the sql:get-rowcount reporter.
 * 
 * @author NetLogo project-team
 *
 */
public class GetRowCount extends DefaultReporter {
 	
 	private final SqlEnvironment sqlenv = SqlExtension.getSqlEnvironment();
 	/**
 	 * Checks syntax of the sql:get-rowcount reporter
 	 * @return syntax object handle
 	 */
 	public Syntax getSyntax() {
 		return Syntax.reporterSyntax( new int[] {}, Syntax.TYPE_NUMBER);
 	}

 	/**
 	 * Executes the sql:fetch-rowcount reporter.
 	 * <p>
 	 * Returns the number of rows affected by an update. Returns <code>0</code> if no rows were changed (or if a SELECT was
 	 * executed). Returns <code>-1</code> if no statement has been executed, or if there is no connection.
 	 * </p> 
 	 * 
 	 * @param args (none)
 	 * @param context
 	 * @throws ExtensionException
 	 * @throws org.nlogo.api.LogoException
 	 */
 	public Object report(Argument args[], Context context)
			throws ExtensionException, org.nlogo.api.LogoException {

 		SqlConnection sqlc = sqlenv.getSqlConnection(context, false);

 		if (sqlc != null) {
 			return sqlc.getRowCount();
 		}
		return Double.valueOf(-1);
 	}
}
