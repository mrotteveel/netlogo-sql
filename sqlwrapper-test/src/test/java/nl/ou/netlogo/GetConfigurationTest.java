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
package nl.ou.netlogo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.nlogo.api.LogoList;
import org.nlogo.nvm.EngineException;

import nl.ou.netlogo.testsupport.ConnectionInformation;
import nl.ou.netlogo.testsupport.HeadlessTest;

/**
 * Test for sql:get-configuration and sql:get-full-configuration.
 * 
 * @author Mark Rotteveel
 */
public class GetConfigurationTest extends HeadlessTest {
	
	private static final Map<String, String> EXPECTED_DEFAULTS;
	static {
		Map<String, String> defaults = new HashMap<String, String>();
		defaults.put("brand", "MySql");
		defaults.put("port", "3306");
		defaults.put("host", "localhost");
		defaults.put("password", "<default, invalid>");
		defaults.put("user", "<default, invalid>");
		defaults.put("database", "<default, invalid>");
		defaults.put("autodisconnect", "on");
		
		EXPECTED_DEFAULTS = Collections.unmodifiableMap(defaults);
	}
	
	/**
	 * Test for default configuration reported for defaultconnection.
	 * <p>
	 * Expected: defaults match expected.
	 * </p>
	 * <p>
	 * Assumptions: expected defaults of test are equal to defaults configured in SqlConfiguration class.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testGetConfiguration_defaultconnection_noconfig() throws Exception {
		workspace.open("init-sql.nlogo");
		
		LogoList resultList = (LogoList)workspace.report("sql:get-configuration \"defaultconnection\"");
		assertEquals("Unexpected configuration name", "defaultconnection", resultList.get(0));
		// Check size of list (1 higher than number of settings)
		assertEquals("Unexpected configuration size", EXPECTED_DEFAULTS.size() + 1, resultList.size());
		for (int idx = 1; idx < resultList.size(); idx++) {
			LogoList valuePair = (LogoList)resultList.get(idx);
			String key = (String) valuePair.get(0);
			String value = (String) valuePair.get(1);
			assertTrue(String.format("Returned key %s not in expected defaults", key), EXPECTED_DEFAULTS.containsKey(key));
			assertEquals(String.format("Key %s has unexpected value", key), EXPECTED_DEFAULTS.get(key), value);
		}
	}
	
	private static final Map<String, String> EXPECTED_CONFIG;
	static {
		Map<String, String> config = new HashMap<String, String>();
		ConnectionInformation ci = ConnectionInformation.getInstance();
		config.put("brand", "MySql");
		config.put("port", ci.getPort());
		config.put("host", ci.getHost());
		config.put("password", ci.getPassword());
		config.put("user", ci.getUsername());
		config.put("database", ci.getSchema());
		config.put("autodisconnect", ci.getAutoDisconnect());
		
		EXPECTED_CONFIG = Collections.unmodifiableMap(config);
	}
	
	/**
	 * Test if sql:get-configuration reports right settings after setting values using sql:configure.
	 * <p>
	 * Expected: values reported are equal to values configured.
	 * </p>
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testGetConfiguration_defaultconnection_withConfig() throws Exception {
		workspace.open("init-sql.nlogo");
		
		ConnectionInformation ci = ConnectionInformation.getInstance();
		workspace.command(
				String.format("sql:configure \"defaultconnection\" [[\"host\" \"%s\"] [\"port\" \"%s\"] [\"user\" \"%s\"] [\"password\" \"%s\"] [\"database\" \"%s\"] [\"autodisconnect\" \"%s\"]]",
				ci.getHost(), ci.getPort(), ci.getUsername(), ci.getPassword(), ci.getSchema(), ci.getAutoDisconnect()));
		
		LogoList resultList = (LogoList)workspace.report("sql:get-configuration \"defaultconnection\"");
		assertEquals("Unexpected configuration name", "defaultconnection", resultList.get(0));
		for (int idx = 1; idx < resultList.size(); idx++) {
			LogoList valuePair = (LogoList)resultList.get(idx);
			String key = (String) valuePair.get(0);
			String value = (String) valuePair.get(1);
			assertTrue(String.format("Returned key %s not in expected configuration", key), EXPECTED_CONFIG.containsKey(key));
			assertEquals(String.format("Key %s has unexpected value", key), EXPECTED_CONFIG.get(key), value);
		}
	}
	
	/**
	 * Test if sql:get-configuration throws an error if called with a non-existent settingname.
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test(expected = EngineException.class)
	public void testGetConfiguration_unknownsetting() throws Exception {
		workspace.open("init-sql.nlogo");
		
		workspace.report("sql:get-configuration \"unknownsetting\"");
	}

	// TODO Decide if it is necessary to create a more extensive test of GetFullConfiguration
	
	/**
	 * Test if sql:get-full-configuration returns a LogoList.
	 * 
	 * @throws Exception For any exceptions during testing
	 */
	@Test
	public void testGetFullConfiguration_default() throws Exception {
		workspace.open("init-sql.nlogo");
		
		Object result = workspace.report("sql:get-full-configuration");
		assertTrue("Unexpected result object type", result instanceof LogoList);
	}

}
