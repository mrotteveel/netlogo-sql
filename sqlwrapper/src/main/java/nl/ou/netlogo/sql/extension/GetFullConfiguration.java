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

import java.util.Iterator;

import nl.ou.netlogo.sql.wrapper.SqlEnvironment;
import nl.ou.netlogo.sql.wrapper.SqlExtension;
import nl.ou.netlogo.sql.wrapper.SqlSetting;

import org.nlogo.api.Argument;
import org.nlogo.api.Context;
import org.nlogo.api.DefaultReporter;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoList;
import org.nlogo.api.Syntax;

/**
 * GetFullConfiguration implements the sql:get-full-configuration command
 * 
 * @author NetLogo project-team
 * 
 */
public class GetFullConfiguration extends DefaultReporter {

    private final SqlEnvironment sqlenv = SqlExtension.getSqlEnvironment();

    /**
     * Checks syntax of the sql:get-full-configuration reporter.
     * 
     * @return syntax object handle
     */
    public Syntax getSyntax() {
        return Syntax.reporterSyntax(new int[] {}, Syntax.TYPE_LIST);
    }

    /**
     * Executes sql:get-full-configuration command.
     * 
     * @param args
     * @param context
     * @return full list of configured entities
     * @throws ExtensionException
     * @throws org.nlogo.api.LogoException
     */
    public Object report(Argument args[], Context context) throws ExtensionException, org.nlogo.api.LogoException {
        LogoList confList = new LogoList();
        try {
            Iterator<String> it = sqlenv.getConfiguration().keySet().iterator();
            // loop over the configured entities by name
            while (it.hasNext()) {
                LogoList conf = new LogoList();
                String name = it.next();
                conf.add(name);
                SqlSetting setting = sqlenv.getConfiguration().get(name);
                if (setting.isVisible()) {
                    Iterator<String> keys = setting.keySet().iterator();
                    // loop over the key-value pairs for a configured entity
                    while (keys.hasNext()) {
                        LogoList kvpair = new LogoList();
                        String key = keys.next();
                        kvpair.add(key);
                        kvpair.add(setting.getString(key));
                        conf.add(kvpair);
                    }
                    confList.add(conf);
                }
            }

        } catch (Exception e) {
            throw new ExtensionException(e);
        }

        return confList;
    }
}
