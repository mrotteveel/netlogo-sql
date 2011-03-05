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
package nl.ou.netlogo.sql.tools;

import java.util.Arrays;

import nl.ou.netlogo.sql.wrapper.SqlExtension;

import org.nlogo.api.Command;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.Primitive;
import org.nlogo.api.PrimitiveManager;
import org.nlogo.api.Syntax;

/**
 * Tool to list the primitives defined in the extension.
 * <p>
 * Generates a list of the primitives and their arguments and return value.
 * Useful for checking documentation after changes.
 * </p>
 * 
 * @author NetLogo project-team
 *
 */
public class ListPrimitives implements PrimitiveManager {

	public static void main(String[] args) throws ExtensionException {
		ListPrimitives lp = new ListPrimitives();
		SqlExtension ext = new SqlExtension();
		ext.load(lp);
	}

	@Override
	public void addPrimitive(String name, Primitive primitive) {
		Syntax syntax = primitive.getSyntax();
		int[] parameterIds = syntax.getRight();
		String[] parameterTypeNames = new String[parameterIds.length];
		for (int idx = 0; idx < parameterIds.length; idx++) {
			parameterTypeNames[idx] = getTypeName(parameterIds[idx]);
		}
		boolean isCommand = primitive instanceof Command;
		String returnType = getTypeName(syntax.getRet());
		System.out.printf("%-8s : %-7s %-20s %s%n", isCommand ? "command" : "reporter",
				returnType, name, Arrays.toString(parameterTypeNames));
	}
	
	/**
	 * Converts the syntax id to a type name.
	 * 
	 * @param id Syntax id
	 * @return Type name
	 */
	private String getTypeName(int id) {
		switch(id) {
		case Syntax.TYPE_BOOLEAN:
			return "boolean";
		case Syntax.TYPE_NUMBER:
			return "number";
		case Syntax.TYPE_LIST:
			return "list";
		case Syntax.TYPE_STRING:
			return "string";
		case Syntax.TYPE_VOID:
			return "void";
		default:
			return "unknown/unsupported id: " + id;
		}
	}

	@Override
	public boolean autoImportPrimitives() {
		return false;
	}

	@Override
	public void autoImportPrimitives(boolean arg0) {
		// Stub: Not implemented
	}
}
