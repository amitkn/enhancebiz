package com.enhancebiz.service.common.conf;

import java.io.Serializable;
import java.sql.Timestamp;

import com.enhancebiz.service.common.constants.DatabaseType;

public class DatabaseInfo implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private int id;
	private String name;
	private String host;
	private int port;
	private String database;
	private String user;
	private String password;
	private boolean useJtds;
	private DatabaseType vendor;
	private Timestamp modifiedTime;
	
	public int getId()
	{
		return id;
	}
	public void setId(int id)
	{
		this.id = id;
	}
	public String getName()
	{
		return name;
	}
	public void setName(String name)
	{
		this.name = name;
	}
	public String getHost()
	{
		return host;
	}
	public void setHost(String host)
	{
		this.host = host;
	}
	public int getPort()
	{
		return port;
	}
	public void setPort(int port)
	{
		this.port = port;
	}
	public String getDatabase()
	{
		return database;
	}
	public void setDatabase(String database)
	{
		this.database = database;
	}
	public String getUser()
	{
		return user;
	}
	public void setUser(String user)
	{
		this.user = user;
	}
	public String getPassword()
	{
		return password;
	}
	public void setPassword(String password)
	{
		this.password = password;
	}
	public boolean isUseJtds()
	{
		return useJtds;
	}
	public void setUseJtds(boolean useJtds)
	{
		this.useJtds = useJtds;
	}
	public DatabaseType getVendor()
	{
		return vendor;
	}
	public void setVendor(DatabaseType vendor)
	{
		this.vendor = vendor;
	}
	public Timestamp getModifiedTime()
	{
		return modifiedTime;
	}
	public void setModifiedTime(Timestamp modifiedTime)
	{
		this.modifiedTime = modifiedTime;
	}
}
