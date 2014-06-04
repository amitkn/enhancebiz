package com.enhancebiz.service.common.db;

import java.lang.reflect.Field;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.enhancebiz.service.common.conf.DatabaseInfo;
import com.enhancebiz.service.common.dto.Column;
import com.enhancebiz.service.common.dto.ServiceException;
import com.enhancebiz.service.common.exception.ExceptionUtil;


public class QueryExecutor
{
	private static final Log logger = LogFactory.getLog(QueryExecutor.class);
	
	private DatabaseInfo dbInfo;
	private String stmtResourcePath;
	
	public QueryExecutor(DatabaseInfo dbInfo, String stmtResourcePath)
	{
		this.dbInfo = dbInfo;
		this.stmtResourcePath = stmtResourcePath;
	}

	public DatabaseInfo getDatabaseInfo()
	{
		return dbInfo;
	}
	public String getStmtResourcePath()
	{
		return stmtResourcePath;
	}
	
	public String getSql(String statementName)
	throws ServiceException
	{
		return StatementReader.read(getStmtResourcePath(), statementName);
	}

	public <T> List<T> execute(String statementName, Class<T> resultClass, Map<String, String> replacedParams, Object... params)
	throws ServiceException
	{
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try
		{
			logger.debug("Analyzing the fields and annotated columns in the Result Class");
			Map<String, Field> sqlFields = getSqlFields(resultClass);
			
			String sql = getSql(statementName);

			logger.debug("Trying to get the db connection ");
			conn = ConnectionUtil.getConnection(dbInfo);
			sql = replaceParamTokens(replacedParams, sql);
			
			logger.debug(String.format("Executing Statement: %1$s", sql));
			
			stmt = conn.prepareStatement(sql);
			applyFilterParams(stmt, params);

			logger.debug(String.format("Filter Parameters Applied: %1$s, Params Count: %2$d", sql, params.length));
			
			List<T> results = new ArrayList<T>();
			rs = stmt.executeQuery();

			logger.debug("SQL Statement Executed Successfully, attempting read the result set");
			
			//find out the result columns that are required in the entity
			Set<String> resultColumns = resolveReadableColumns(rs, sqlFields);
			while (rs.next())
			{
				T result = resultClass.newInstance();
				results.add(result);
				for (Map.Entry<String, Field> entry : sqlFields.entrySet())
				{
					//read only columns that exists in the entity as well as
					//in the result column
					if (resultColumns.contains(entry.getKey()))
					{
						//handle the fields which are enumerations
						Field field = entry.getValue();
						setFieldValue(field, result, rs, entry.getKey());
					}
				}
			}
			
			logger.debug(String.format("SQL Result has read successfully, and total results: %1$d", results.size()));
			return results;
		}
		catch (Throwable e)
		{
			String msg = String.format("Statement: %1$s, DbType: %2$s failed with an exception: %3$s", statementName, 
					dbInfo.getName(), e.getMessage());
			logger.error(msg, e);
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_FAILED, e);
		}
		finally
		{
			ConnectionUtil.closeResultSet(rs);
			ConnectionUtil.closeStatement(stmt);
			ConnectionUtil.closeConnection(conn);
		}
	}
	
	public <T> List<T> executeStoredProcedure(String statementName, Class<T> resultClass, Map<String, String> replacedParams, Object... params)
	throws ServiceException
	{
		Connection conn = null;
		CallableStatement stmt = null;
		ResultSet rs = null;
		try
		{
			logger.debug("Analyzing the fields and annotated columns in the Result Class");
			Map<String, Field> sqlFields = getSqlFields(resultClass);
			
			String sql = getSql(statementName);

			logger.debug("Trying to get the db connection ");
			conn = ConnectionUtil.getConnection(dbInfo);
			sql = replaceParamTokens(replacedParams, sql);
			
			logger.debug(String.format("Executing Statement: %1$s", sql));
			
			stmt = conn.prepareCall(sql);
			applyFilterParams(stmt, params);

			logger.debug(String.format("Filter Parameters Applied: %1$s, Params Count: %2$d", sql, params.length));
			
			List<T> results = new ArrayList<T>();
			rs = stmt.executeQuery();

			logger.debug("SQL Statement Executed Successfully, attempting read the result set");
			
			//find out the result columns that are required in the entity
			Set<String> resultColumns = resolveReadableColumns(rs, sqlFields);
			while (rs.next())
			{
				T result = resultClass.newInstance();
				results.add(result);
				for (Map.Entry<String, Field> entry : sqlFields.entrySet())
				{
					//read only columns that exists in the entity as well as
					//in the result column
					if (resultColumns.contains(entry.getKey()))
					{
						//handle the fields which are enumerations
						Field field = entry.getValue();
						setFieldValue(field, result, rs, entry.getKey());
					}
				}
			}
			
			logger.debug(String.format("SQL Result has read successfully, and total results: %1$d", results.size()));
			return results;
		}
		catch (Throwable e)
		{
			String msg = String.format("Statement: %1$s, DbType: %2$s failed with an exception: %3$s", statementName, 
					dbInfo.getName(), e.getMessage());
			logger.error(msg, e);
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_FAILED, e);
		}
		finally
		{
			ConnectionUtil.closeResultSet(rs);
			ConnectionUtil.closeStatement(stmt);
			ConnectionUtil.closeConnection(conn);
		}
	}
	
	
	public Map<String, Object> executeSingleMap(String statementName, Map<String, String> replacedParams, Object... params)
	throws ServiceException
	{
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try
		{
			String sql = getSql(statementName);
			conn = ConnectionUtil.getConnection(dbInfo);
			sql = replaceParamTokens(replacedParams, sql);
			
			logger.debug(String.format("Executing Statement: %1$s", sql));
			
			stmt = conn.prepareStatement(sql);
			applyFilterParams(stmt, params);

			logger.debug(String.format("Filter Parameters Applied: %1$s, Params Count: %2$d", sql, params.length));

			Map<String, Object> map = new HashMap<String, Object>();
			rs = stmt.executeQuery();

			logger.debug("SQL Statement Executed Successfully, attempting read the result set");
			
			//find out the result columns that are required in the entity
			ResultSetMetaData metaData = rs.getMetaData();
			if (rs.next())
			{
				for (int i = 1; i <= metaData.getColumnCount(); i++)
					map.put(metaData.getColumnLabel(i), rs.getObject(i));
			}
			
			logger.debug(String.format("SQL Result has read successfully, and total Fields: %1$d", map.size()));
			return map;
		}
		catch (Exception e)
		{
			String msg = String.format("Statement: %1$s, DbType: %2$s failed with an exception: %3$s", statementName, 
					dbInfo.getName(), e.getMessage());
			logger.error(msg, e);
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_FAILED, e);
		}
		finally
		{
			ConnectionUtil.closeResultSet(rs);
			ConnectionUtil.closeStatement(stmt);
			ConnectionUtil.closeConnection(conn);
		}
	}

	public int executeScalar(String statementName, Map<String, String> replacedParams, Object... params)
	throws ServiceException
	{
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try
		{
			String sql = getSql(statementName);
			conn = ConnectionUtil.getConnection(dbInfo);
			sql = replaceParamTokens(replacedParams, sql);
			
			logger.debug(String.format("Executing Statement: %1$s", sql));
			
			stmt = conn.prepareStatement(sql);
			applyFilterParams(stmt, params);

			logger.debug(String.format("Filter Parameters Applied: %1$s, Params Count: %2$d", sql, params.length));
			
			int ret = 0;
			rs = stmt.executeQuery();
			if (rs.next())
				ret = rs.getInt(1);

			logger.debug("SQL Statement Executed Successfully, attempting read the result set");
			return ret;
		}
		catch (Exception e)
		{
			String msg = String.format("Statement: %1$s, DbType: %2$s failed with an exception: %3$s", statementName, 
					dbInfo.getName(), e.getMessage());
			logger.error(msg, e);
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_FAILED, e);
		}
		finally
		{
			ConnectionUtil.closeResultSet(rs);
			ConnectionUtil.closeStatement(stmt);
			ConnectionUtil.closeConnection(conn);
		}
	}
	
	
	public int executeUpdate(String statementName, Map<String, String> replacedParams, Object... params)
	throws ServiceException
	{
		Connection conn = null;
		PreparedStatement stmt = null;
		try
		{
			String sql = getSql(statementName);
			conn = ConnectionUtil.getConnection(dbInfo);
			sql = replaceParamTokens(replacedParams, sql);
			
			logger.debug(String.format("Executing Statement: %1$s", sql));
			
			stmt = conn.prepareStatement(sql);
			applyFilterParams(stmt, params);

			logger.debug(String.format("Filter Parameters Applied: %1$s, Params Count: %2$d", sql, params.length));
			int ret = stmt.executeUpdate();
			logger.debug(String.format("SQL Statement Executed Successfully, affected count: %d", ret));
			return ret;
		}
		catch (Exception e)
		{
			String msg = String.format("Statement: %1$s, DbType: %2$s failed with an exception: %3$s", statementName, 
					dbInfo.getName(), e.getMessage());
			logger.error(msg, e);
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_FAILED, e);
		}
		finally
		{
			ConnectionUtil.closeStatement(stmt);
			ConnectionUtil.closeConnection(conn);
		}
	}

	/**
	 * executeInsert : Inserts single record and returns auto-generated database id
	 * @param statementName
	 * @param replacedParams
	 * @param params
	 * @return returns database generated id
	 * @throws ServiceException
	 */
	public int executeInsert(String statementName, Map<String, String> replacedParams, Object... params)
	throws ServiceException
	{
		Connection conn = null;
		PreparedStatement stmt = null;
		try
		{
			String sql = getSql(statementName);
			conn = ConnectionUtil.getConnection(dbInfo);
			sql = replaceParamTokens(replacedParams, sql);
			
			logger.debug(String.format("Executing Statement: %1$s", sql));
			
			stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			applyFilterParams(stmt, params);

			logger.debug(String.format("Filter Parameters Applied: %1$s, Params Count: %2$d", sql, params.length));
			int ret = stmt.executeUpdate();
			
			ResultSet rs = stmt.getGeneratedKeys();
			if (rs.next())
				ret = rs.getInt(1);
			
			logger.debug(String.format("SQL Statement Executed Successfully, affected count: %d", ret));
			return ret;
		}
		catch (Exception e)
		{
			String msg = String.format("Statement: %1$s, DbType: %2$s failed with an exception: %3$s", statementName, 
					dbInfo.getName(), e.getMessage());
			logger.error(msg, e);
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_FAILED, e);
		}
		finally
		{
			ConnectionUtil.closeStatement(stmt);
			ConnectionUtil.closeConnection(conn);
		}
	}	
	
	public <T> Long insert(String tableName, T rec)
	throws ServiceException
	{
		List<T> records = new ArrayList<T>();
		records.add(rec);
		List<Long> listKeys = insert(tableName, records);
		return listKeys.get(0);
	}
	
	public <T> List<Long> insert(String tableName, List<T> records)
	throws ServiceException
	{
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String sql = null;
		List<Long> listKeys = new ArrayList<Long>();
		try
		{
			if (records == null || records.size() == 0)
				return null;
			
			conn = ConnectionUtil.getConnection(dbInfo);
			conn.setAutoCommit(false);
			
			Field pkField = null;
			Field lockField = null;
			String sqlParam = null;
			Map<String, Field> sqlFields = getSqlFields(records.get(0).getClass());
			for (Map.Entry<String, Field> entry : sqlFields.entrySet())
			{
				Column column = entry.getValue().getAnnotation(Column.class);
				if (column.autoGenerated())
				{
					pkField = entry.getValue();
				}
				else if (!column.selectOnly())
				{
					sql = sql == null ? entry.getKey() : String.format("%s, %s",  sql, entry.getKey());
					sqlParam = sqlParam == null ? "?" : String.format("%s, ?",  sqlParam, entry.getKey());
					
					if (column.optimisticLock())
						lockField = entry.getValue();
				}
			}
			
			// format the INSERT statement
			sql = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, sql, sqlParam);
			logger.debug(String.format("Executing Statement: %1$s", sql));
			stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

			//set the parameter values
			for (T rec : records)
			{
				int indx = 1;
				for (Map.Entry<String, Field> entry : sqlFields.entrySet())
				{
					//set the timestamp to the optimistic lock field
					if (lockField == entry.getValue())
						lockField.set(rec, new Timestamp(System.currentTimeMillis()));
						
					Column column = entry.getValue().getAnnotation(Column.class);
					if (!column.autoGenerated() && !column.selectOnly())
					{
						Object value = entry.getValue().get(rec);
						if (value != null && value.getClass().isEnum())
							stmt.setString(indx++, value.toString());
						else
							stmt.setObject(indx++, value);
					}
				}
				stmt.addBatch();
			}
			
			//execute the 
			stmt.executeBatch();
			rs = stmt.getGeneratedKeys();
			
			for (T rec : records)
			{
				rs.next();
				pkField.set(rec, rs.getLong(1));
				listKeys.add(rs.getLong(1));
			}
			conn.commit();

			logger.debug("Records Inserted successfully");
		}
		catch (Exception e)
		{
			String msg = String.format("Statement: %1$s, DbType: %2$s failed with an exception: %3$s", sql, 
					dbInfo.getName(), e.getMessage());
			logger.error(msg, e);
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_FAILED, e);
		}
		finally
		{
			ConnectionUtil.closeResultSet(rs);
			ConnectionUtil.closeStatement(stmt);
			ConnectionUtil.closeConnection(conn);
		}
		return listKeys;
	}
	
	public <T> void update(String tableName, T rec)
	throws ServiceException
	{
		List<T> records = new ArrayList<T>();
		records.add(rec);
		update(tableName, records);
	}
	
	public <T> void update(String tableName, List<T> records)
	throws ServiceException
	{
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String sql = null;
		try
		{
			if (records == null || records.size() == 0)
				return;
			
			conn = ConnectionUtil.getConnection(dbInfo);
			conn.setAutoCommit(false);
			
			Field pkField = null;
			Field lockField = null;
			
			Map<String, Field> sqlFields = getSqlFields(records.get(0).getClass());
			for (Map.Entry<String, Field> entry : sqlFields.entrySet())
			{
				Column column = entry.getValue().getAnnotation(Column.class);
				if (column.autoGenerated())
				{
					pkField = entry.getValue();
				}
				else
				{
					sql = sql == null ? String.format("%s = ?", entry.getKey()) : String.format("%s, %s = ?",  sql, entry.getKey());
					if (column.optimisticLock())
						lockField = entry.getValue();
				}
			}
			
			// format the where clause for the update
			String whereClause = String.format("%s = ?", pkField.getAnnotation(Column.class).value());
			if (lockField != null)
				whereClause += String.format(" AND %s = ?", lockField.getAnnotation(Column.class).value());
			
			//build the update clause
			sql = String.format("UPDATE %s SET %s WHERE %s", tableName, sql, whereClause);
			logger.debug(String.format("Executing Statement: %1$s", sql));
			stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

			//set the parameter values
			for (T rec : records)
			{
				int indx = 1;
				Timestamp currentTimestamp = (Timestamp) (lockField != null ? lockField.get(rec) : null);
				if (lockField != null) lockField.set(rec, new Timestamp(System.currentTimeMillis()));
				
				for (Map.Entry<String, Field> entry : sqlFields.entrySet())
				{
					Column column = entry.getValue().getAnnotation(Column.class);
					if (!column.autoGenerated())
					{		
						Object value = entry.getValue().get(rec);
						if (value != null && value.getClass().isEnum())
							stmt.setString(indx++, value.toString());
						else
							stmt.setObject(indx++, value);
					}
				}
				stmt.setObject(indx++, pkField.get(rec));
				if (lockField != null)
					stmt.setObject(indx++, currentTimestamp);
				
				stmt.addBatch();
			}
			
			//execute the 
			stmt.executeBatch();
			conn.commit();

			logger.debug("Records Updated successfully");
		}
		catch (Exception e)
		{
			String msg = String.format("Statement: %1$s, DbType: %2$s failed with an exception: %3$s", sql, 
					dbInfo.getName(), e.getMessage());
			logger.error(msg, e);
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_FAILED, e);
		}
		finally
		{
			ConnectionUtil.closeResultSet(rs);
			ConnectionUtil.closeStatement(stmt);
			ConnectionUtil.closeConnection(conn);
		}
	}
	
	/**
	 * Returns the annotated fields which are exists in the 
	 * SqlResultSet
	 */
	private Set<String> resolveReadableColumns(ResultSet rs,
			Map<String, Field> sqlFields) 
	throws SQLException
	{
		ResultSetMetaData metaData = rs.getMetaData();
		Set<String> resultColumns = new HashSet<String>();
		for (int i = 0; i < metaData.getColumnCount(); i++)
		{
			String column = metaData.getColumnLabel(i + 1);
			if (sqlFields.containsKey(column))
				resultColumns.add(column);
		}
		return resultColumns;
	}

	/**
	 * Apply Filter parameters if there any
	 * 
	 * @param stmt prepared statement
	 * @param params parameters collection, applied in the order that the parameter place holders exists in
	 * the statement
	 * @throws SQLException
	 */
	private void applyFilterParams(PreparedStatement stmt, Object... params)
		throws SQLException
	{
		int index = 1;
		for (Object param: params)
		{
			stmt.setObject(index++, param);
		}
	}

	/**
	 * Replace the replaceable tokens in the Query String like 
	 * "@token1@
	 * 
	 * @param replacedParams value for the replaceable tokens in the Sql
	 * @param sql SQL Statement
	 * @return replace sql statement
	 */
	private String replaceParamTokens(Map<String, String> replacedParams,
			String sql)
	{
		if (replacedParams != null)
		{
			for (Map.Entry<String, String> entry : replacedParams.entrySet())
			{
				sql = sql.replaceAll(entry.getKey(), entry.getValue());
			}
		}
		return sql;
	}

	/**
	 * Returns the Column annotated columns
	 * 
	 * @param <T> classType
	 * @param resultClass result class
	 * @return fields which are annotated
	 * @throws ServiceException
	 */
	private <T> Map<String, Field> getSqlFields(Class<T> resultClass)
		throws ServiceException
	{
		//get the declare fields in the Result Class, if there are no fields
		//defined throw an exception
		List<Field> fields = new LinkedList<Field>();
		readFields(fields, resultClass);
		if (fields.size() == 0)
		{
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_NO_FIELDS);
		}
		
		//get the fields which are annotated and create collection to be used
		//later in the execute method
		Map<String, Field> sqlFields = new LinkedHashMap<String, Field>();
		for (Field field : fields)
		{
			field.setAccessible(true);
			if (field.isAnnotationPresent(Column.class))
			{
				Column column = field.getAnnotation(Column.class);
				sqlFields.put(column.value(), field);
			}
		}
		if (sqlFields.size() == 0)
		{
			throw ExceptionUtil.generateServiceException(SqlError.QUERY_EXECUTOR_NO_ANNOTATIONS);
		}
		return sqlFields;
	}

	private void readFields(List<Field> fieldList, Class<?> resultClass)
	{
		if ( resultClass != null)
		{
			Field[] fields = resultClass.getDeclaredFields();
			fieldList.addAll(Arrays.asList(fields));
			Class<?> superClass = resultClass.getSuperclass();
			readFields(fieldList, superClass);
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> void setFieldValue(Field field, T result, ResultSet rs, String fieldName) throws Exception
	{
		Class<?> type = field.getType();

		if (type == String.class)
		{
			String value = rs.getString(fieldName);
			field.set(result, value);
		}
		else if (type == Date.class)
		{
			Date value = rs.getTimestamp(fieldName);
			field.set(result, value);
		}
		else if (type == Integer.class || type == int.class)
		{
			Integer value = rs.getInt(fieldName);
			field.set(result, value);
		}
		else if (type == Long.class || type == long.class)
		{
			Long value = rs.getLong(fieldName);
			field.set(result, value);
		}
		else if (type == Float.class || type == float.class)
		{
			Float value = rs.getFloat(fieldName);
			field.set(result, value);
		}
		else if (type == Double.class || type == double.class)
		{
			Double value = rs.getDouble(fieldName);
			field.set(result, value);
		}
		else if (type == Boolean.class || type == boolean.class) 
		{
			Boolean value = rs.getBoolean(fieldName);
			field.set(result, value);
		}
		else if (field.getType().isEnum())
		{
			Object value = rs.getObject(fieldName);
			if (value != null)
				field.set(result, Enum.valueOf((Class<Enum>) field.getType(), (String) value));
		}
	}
}
  