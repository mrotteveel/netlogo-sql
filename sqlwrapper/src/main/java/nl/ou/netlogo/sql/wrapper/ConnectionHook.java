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

import java.util.logging.Level;

import com.jolbox.bonecp.ConnectionHandle;
import com.jolbox.bonecp.hooks.AbstractConnectionHook;

/**
 * ConnectionHook implementation to reset autocommit status of connections
 * returned to pool.
 * 
 * @author NetLogo project-team
 */
public class ConnectionHook extends AbstractConnectionHook {

    @Override
    public void onCheckIn(ConnectionHandle connection) {
        try {
            boolean autoCommit = connection.getInternalConnection().getAutoCommit();
            if (!autoCommit) {
                try {
                    connection.getInternalConnection().setAutoCommit(true);
                } catch (Exception ex) {
                    SqlLogger.getLogger().log(Level.FINEST, "Unable to reset autocommit status", ex);
                }
            }
            // connection.setAutoCommit(true);
        } catch (Exception ex) {
            SqlLogger.getLogger().log(Level.FINEST, "Unable to get autocommit status", ex);
        }
    }
}
