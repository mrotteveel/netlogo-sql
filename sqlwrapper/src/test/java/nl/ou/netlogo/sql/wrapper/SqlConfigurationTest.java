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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.nlogo.api.ExtensionException;
import org.nlogo.api.LogoList;
import org.nlogo.api.LogoListBuilder;

public class SqlConfigurationTest {
	
	/**
	 * Test for {@link SqlConfiguration#parseSettingList(String, LogoList)} for an empty list.
	 * <p>
	 * Expected: empty map returned
	 * </p>
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testParseSettingsList_empty() throws Exception {
		Map<String, String> settings = SqlConfiguration.parseSettingList("name", new LogoListBuilder().toLogoList());
		
		assertEquals("Expected empty map as result", Collections.emptyMap(), settings);
	}
	
	/**
	 * Test for {@link SqlConfiguration#parseSettingList(String, LogoList)} for a list with a single named setting.
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testParseSettingsList_singleNamedSetting() throws Exception {
		LogoListBuilder list = new LogoListBuilder();
		LogoListBuilder singleSetting = new LogoListBuilder();
		singleSetting.add("single_setting_name");
		singleSetting.add("single_setting_value");
		list.add(singleSetting.toLogoList());
		
		Map<String, String> settings = SqlConfiguration.parseSettingList("name", list.toLogoList());
		
		Map<String, String> expectedMap = new HashMap<String, String>();
		expectedMap.put("single_setting_name", "single_setting_value");
		
		assertEquals("Settings has unexpected content", expectedMap, settings);
	}
	
	/**
	 * Test for {@link SqlConfiguration#parseSettingList(String, LogoList)} for a list with a multiple named settings.
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testParseSettingsList_multipleNamedSettings() throws Exception {
		LogoListBuilder list = new LogoListBuilder();
		LogoListBuilder setting = new LogoListBuilder();
		setting.add("setting_name_1");
		setting.add("setting_value_1");
		list.add(setting.toLogoList());
		setting = new LogoListBuilder();
		setting.add("setting_name_2");
		setting.add("setting_value_2");
		list.add(setting.toLogoList());
		
		Map<String, String> settings = SqlConfiguration.parseSettingList("name", list.toLogoList());
		
		Map<String, String> expectedMap = new HashMap<String, String>();
		expectedMap.put("setting_name_1", "setting_value_1");
		expectedMap.put("setting_name_2", "setting_value_2");
		
		assertEquals("Settings has unexpected content", expectedMap, settings);
	}
	
	/**
	 * Test for {@link SqlConfiguration#parseSettingList(String, LogoList)} for a list with a single list entry (unnamed setting)
	 * 
	 * @throws Exception (should not occur)
	 */
	@Test
	public void testParseSettingsList_singleUnnamedSetting() throws Exception {
		LogoListBuilder list = new LogoListBuilder();
		LogoListBuilder singleSetting = new LogoListBuilder();
		singleSetting.add("single_setting");
		list.add(singleSetting.toLogoList());
		
		Map<String, String> settings = SqlConfiguration.parseSettingList("name", list.toLogoList());
		
		Map<String, String> expectedMap = new HashMap<String, String>();
		expectedMap.put("", "single_setting");
		
		assertEquals("Settings has unexpected content", expectedMap, settings);
	}
	
	/**
	 * Test for {@link SqlConfiguration#parseSettingList(String, LogoList)} with a list entry that has too many items.
	 * 
	 * @throws Exception Expected ExtensionException
	 */
	@Test(expected=ExtensionException.class)
	public void testParseSettingsList_settingListTooBig() throws Exception {
		LogoListBuilder list = new LogoListBuilder();
		LogoListBuilder singleSetting = new LogoListBuilder();
		singleSetting.add("single_setting_name");
		singleSetting.add("single_setting_value");
		singleSetting.add("extra_entry");
		list.add(singleSetting.toLogoList());
		
		SqlConfiguration.parseSettingList("name", list.toLogoList());
	}
	
	/**
	 * Test for {@link SqlConfiguration#parseSettingList(String, LogoList)} with multiple entries and a list entry that has too few items.
	 * 
	 * @throws Exception Expected ExtensionException
	 */
	@Test(expected=ExtensionException.class)
	public void testParseSettingsList_multipleSetting_settingListTooSmall() throws Exception {
		LogoListBuilder list = new LogoListBuilder();
		LogoListBuilder setting = new LogoListBuilder();
		setting.add("setting_name_1");
		setting.add("setting_value_1");
		list.add(setting.toLogoList());
		setting = new LogoListBuilder();
		setting.add("setting_name_no_value");
		list.add(setting.toLogoList());
		
		SqlConfiguration.parseSettingList("name", list.toLogoList());
	}
	
	// TODO Consider case of an invalid settings list (eg list containing other objects than LogoList)
	
	// TODO Test other aspects of SqlConfiguration

}
