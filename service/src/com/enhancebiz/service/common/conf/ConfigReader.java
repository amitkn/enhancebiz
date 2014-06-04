/**
 * 
 */
package com.enhancebiz.service.common.conf;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;

/**
 * @author manishk
 *
 */
public class ConfigReader
{
	private static Log logger = LogFactory.getLog(ConfigReader.class);
	private static final String CONFIG_FILE_PATH = "/Users/manishk/project/conf/";	
	private static final String SYSTEM_PROPERTY_LIGHTSPEED_CONF = "spoken.srm.conf";
	
	private static final String PROPERTY_DATABASES = "databases";
	
	// collection
	private static Properties props;

	public ConfigReader(String configFileName) throws Exception
	{
		String configPath = CONFIG_FILE_PATH;
		try
		{
			configPath = System.getProperty(SYSTEM_PROPERTY_LIGHTSPEED_CONF, CONFIG_FILE_PATH);
			File configFile = new File(configPath, configFileName);
			if (!configFile.exists())
				throw new Exception(String.format("Configuration [%s] does not exists", configFile.getAbsolutePath()));

			props = new Properties();
			props.load(new FileInputStream(configFile));
		}
		catch (Exception e)
		{
			String msg = String
					.format("Configuration lightspeed-image.conf failed to load from [Path: %s], [File: %s]",
							configPath, configFileName);
			logger.error(msg, e);
			throw e;
		}
	}

	public DatabaseInfo[] getDatabases() throws Exception
	{
		String value = readProperty(PROPERTY_DATABASES);
		Gson gson = new Gson();
		DatabaseInfo[] dbs = gson.fromJson(value, DatabaseInfo[].class);
		return dbs;
	}
	
	private static String readProperty(String propName) throws Exception
	{
		String value = props.getProperty(propName);
		if (value == null)
			throw new Exception(String.format("Property not found: [Name: %s]",  propName));
		return value;
	}
}
