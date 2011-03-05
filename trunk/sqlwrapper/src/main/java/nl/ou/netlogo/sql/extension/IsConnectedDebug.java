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

import org.nlogo.api.Agent;
import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.ExtensionException;

/**
 * Debug implementation of is-connected? which will return false if a pooled connection is not established.
 * 
 * @author NetLogo project-team
 *
 */
public class IsConnectedDebug extends IsConnected {
	public Object report(Argument args[], Context context) throws ExtensionException , org.nlogo.api.LogoException {

	Agent agent = context.getAgent();
	
	if ( agent == null ) {
		return (false);
	}
	
	//
	// We use getSqlConnection() and not getActiveSqlConnection()
	// as the latter will throw an exception when there is no
	// connection found. From this command context we just
	// want to return false for that scenario.
	//
	SqlConnection sqlc = sqlenv.getSqlConnection(context, false);

	if ( sqlc == null ) {
		return false;
	}
	else {
		return sqlc.isConnected();
	}
}
}
