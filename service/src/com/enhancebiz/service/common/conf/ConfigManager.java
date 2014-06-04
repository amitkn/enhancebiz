/**
 * 
 */
package com.enhancebiz.service.common.conf;

import java.util.HashMap;
import java.util.Map;

/**
 * @author manishk
 *
 */
public class ConfigManager
{
	private static final String CONFIG_FILENAME_CONF = "medical.conf";
	
	private static Map<String, DatabaseInfo> databaseMap;
	
	public static DatabaseInfo getDatabase(String name) throws Exception
	{
		if (databaseMap == null)
		{
			synchronized (ConfigManager.class)
			{
				if (databaseMap == null)
				{
					databaseMap = new HashMap<String, DatabaseInfo>();
					ConfigReader reader = new ConfigReader(CONFIG_FILENAME_CONF);
					for (DatabaseInfo dbInfo : reader.getDatabases())
						databaseMap.put(dbInfo.getName(), dbInfo);
				}
			}
		}
		return databaseMap.get(name);
	}
}
