/**
 * 
 */
package com.enhancebiz.service.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

import org.apache.commons.dbcp.datasources.SharedPoolDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.enhancebiz.service.common.conf.DatabaseInfo;
import com.enhancebiz.service.common.constants.DatabaseType;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

/**
 * @author manishk
 *
 */
public class ConnectionUtil 
{
	private static Log logger = LogFactory.getLog(ConnectionUtil.class);
	private static final Map<String, DataSource> mysqlDataSourceMap = new HashMap<String, DataSource>();
	
	public static Connection getConnection(DatabaseInfo dbInfo)
	throws Exception
	{
		try
		{
			Connection conn = null;
			if (dbInfo.getVendor() == DatabaseType.service)
				conn = getMySqlConnection(dbInfo);
			return conn;
		}
		catch (Exception e)
		{
			String msg = String.format("Connection creation failed, %1$s", dbInfo.getVendor());
			logger.error(msg, e);
			throw e;
		}
	}

	public static void closeConnection(Connection conn)
	{
		try
		{
			if (conn != null)
				conn.close();
		}
		catch (Exception e)
		{
			logger.error(e);
		}
	}
	
	public static void closeStatement(PreparedStatement stmt)
	{
		try
		{
			if (stmt != null)
				stmt.close();
		}
		catch (Exception e)
		{
			logger.error(e);
		}
	}

	public static void closeResultSet(ResultSet rs)
	{
		try
		{
			if (rs != null)
				rs.close();
		}
		catch (Exception e)
		{
			logger.error(e);
		}
	}
	
	public static Connection getMySqlConnection(DatabaseInfo database)
			throws Exception
	{
		try
		{
			DataSource dataSource = createMySqlDataSource(database);
			return dataSource.getConnection();
		}
		catch (Exception e)
		{
			String msg = String
					.format("MySQL database connection creation has failed: [Host: %s], [Database: %s], [Port:%d], [User: %s]",
							database.getHost(), database.getDatabase(),
							database.getPort(), database.getUser());
			logger.error(msg, e);
			throw e;
		}
	}
	
	public static DataSource createMySqlDataSource(DatabaseInfo database)
			throws Exception
	{
		String dsName = String.format("%s.%s", database.getHost(), database.getDatabase());
		DataSource ds = mysqlDataSourceMap.get(dsName);
		if (ds == null)
		{
			logger.debug(String.format("DataSource: %s is not found in the MySQL DataSource Cache Map", dsName));
			synchronized (mysqlDataSourceMap)
			{
				ds = mysqlDataSourceMap.get(dsName);
				if (ds == null)
				{
					MysqlConnectionPoolDataSource dataSource = new MysqlConnectionPoolDataSource();
					dataSource.setServerName(database.getHost());
					dataSource.setPort(database.getPort());
					dataSource.setDatabaseName(database.getDatabase());
					dataSource.setUser(database.getUser());
					dataSource.setPassword(database.getPassword());
					dataSource.setAutoReconnect(true);
					dataSource.setAutoReconnectForConnectionPools(true);
					dataSource.setZeroDateTimeBehavior("convertToNull");

					// these two are required max performance
					dataSource.setUseServerPreparedStmts(false);
					dataSource.setRewriteBatchedStatements(true);
					dataSource.setRelaxAutoCommit(false);

					// wrap it with the DBCP pooling datasource
					SharedPoolDataSource pooledDatasource = createSharedDataSource(dataSource);
					pooledDatasource.setValidationQuery("select 1");
					mysqlDataSourceMap.put(dsName, pooledDatasource);
					ds = pooledDatasource;
					logger.debug(String.format("DataSource: %s has been created and cached for future lookups", dsName));
				}
			}
		}
		return ds;
	}
	
	private static SharedPoolDataSource createSharedDataSource(ConnectionPoolDataSource dataSource)
	{
		SharedPoolDataSource pooledDatasource = new SharedPoolDataSource();
		pooledDatasource.setConnectionPoolDataSource(dataSource);
		pooledDatasource.setMaxIdle(1);
		pooledDatasource.setTestWhileIdle(true);
		pooledDatasource.setMinEvictableIdleTimeMillis(1000 * 60 * 1);
		pooledDatasource.setMaxActive(15);
		pooledDatasource.setMaxWait(10);
		pooledDatasource.setValidationQuery("select 1");
		return pooledDatasource;
	}
}
