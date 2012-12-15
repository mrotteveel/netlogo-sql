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

import java.util.Map;

import nl.ou.netlogo.sql.wrapper.SqlConfiguration;
import nl.ou.netlogo.sql.wrapper.SqlEnvironment;
import nl.ou.netlogo.sql.wrapper.SqlExtension;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultCommand;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Syntax;

/**
 * Configure implements the sql:configure command
 * 
 * @author NetLogo project-team
 * 
 */
public class Configure extends DefaultCommand {

    private final SqlEnvironment sqlenv = SqlExtension.getSqlEnvironment();

    /**
     * Checks syntax of the sql:configure command.
     * 
     * @return syntax object handle
     */
    public Syntax getSyntax() {
        return Syntax.commandSyntax(new int[] { Syntax.StringType(), Syntax.ListType() });
    }

    /**
     * Executes sql:configure command.
     * 
     * @param args
     *            <dl>
     *            <dt>args[0]</dt>
     *            <dd>is the name of the item to configure</dd>
     *            <dt>args[1]</dt>
     *            <dd>is the list of key/value pairs to configure</dd>
     *            </dl>
     * @param context
     * @throws ExtensionException
     * @throws org.nlogo.api.LogoException
     */
    public void perform(Argument args[], Context context) throws ExtensionException, org.nlogo.api.LogoException {
        String name = args[0].getString();
        Map<String, String> kvPairs = SqlConfiguration.parseSettingList(name, args[1].getList());
        sqlenv.getConfiguration().setConfiguration(name, kvPairs, context);
    }
}
