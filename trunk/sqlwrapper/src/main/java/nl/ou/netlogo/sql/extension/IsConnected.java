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
import nl.ou.netlogo.sql.wrapper.SqlConnectionManager;

import org.nlogo.api.Argument;
import org.nlogo.api.Agent;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultReporter;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;

/**
 * Class associated with the isConnected? command in a NetLogo model from the SQL extension.
 * 
 * @author NetLogo project-team
 *
 */
public class IsConnected extends DefaultReporter {
	
	protected final SqlEnvironment sqlenv = SqlExtension.getSqlEnvironment();
	
	/**
 	 * Checks syntax of the sql:is-connected? command.
 	 * @return syntax object handle
 	 */
	public Syntax getSyntax() {
		return Syntax.reporterSyntax(Syntax.TYPE_BOOLEAN);
	}
	
	/**
 	 * Executes sql:is-connected? command from model context.
 	 * 
 	 * @param args
 	 * @param context
 	 * @throws ExtensionException
 	 * @throws org.nlogo.api.LogoException
 	 */
	public Object report(Argument args[], Context context)
		throws ExtensionException , org.nlogo.api.LogoException {

		//
		// Do note that if there is a pool available using
		// default connection properties this function
		// always returns true.
		//
		Agent agent = context.getAgent();
		
		if ( agent == null ) {
			return (false);
		}
		
		// When we use connection pooling we are implicitly always
		// connected, hence we return TRUE. Even if there is no
		// physical connection for this task.
		SqlConnectionManager sqlmgr = sqlenv.getConnectionManager();
		
		if ( sqlmgr != null && sqlmgr.connectionPoolEnabled() ) {
			return (true);
		}
		else {
			//
			// We use getSqlConnection() and not getActiveSqlConnection()
			// as the latter will throw an exception when there is no
			// connection found. From this command context we just
			// want to return false for that scenario.
			//
			SqlConnection sqlc = sqlenv.getSqlConnection(context, true);

			if ( sqlc == null ) {
				return false;
			}
			else {
				return sqlc.isConnected();
			}
		}
	}
}
