package com.enhancebiz.service.common.db;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.enhancebiz.service.common.dto.ServiceException;
import com.enhancebiz.service.common.exception.ExceptionUtil;


public class StatementReader
{
	private static final Log logger = LogFactory.getLog(StatementReader.class);
	
	private static final Map<String, Map<String, String>> cachedStatements = new HashMap<String, Map<String, String>>(); 
	
	public static String read(String resourcePath, String statementName)
	throws ServiceException
	{
		String sql = null;
		try
		{
			loadStatements(resourcePath);
			Map<String, String> stmtsMap = cachedStatements.get(resourcePath);
			sql = stmtsMap.get(statementName);
			if (sql == null)
			{
				logger.error(String.format("%2$s could not found in %1$s", resourcePath, statementName));
				throw ExceptionUtil.generateServiceException(SqlError.STMT_READER_NOT_FOUND);
			}
			logger.debug(String.format("Name: %1$s, Sql: %2$s", statementName, sql));
		}
		catch (Exception e)
		{
			logger.error(String.format("%2$s lookup in the %1$s failed wth an exception: %3$s", 
					resourcePath, statementName, e.getMessage()), e);
			throw ExceptionUtil.generateServiceException(SqlError.STMT_READER_PARSE_FAILED, e);
		}
		return sql;
	}

	private static void loadStatements(String resourcePath)
	throws Exception
	{
		if (cachedStatements.get(resourcePath) == null)
		{
			synchronized (cachedStatements)
			{
				if (cachedStatements.get(resourcePath) == null)
				{
					logger.debug(String.format("loading statements from %1$s", resourcePath));
					
					JAXBContext ctx = JAXBContext.newInstance(new Class[] {SqlStatements.class});
					Unmarshaller um = ctx.createUnmarshaller();
					InputStream stream = StatementReader.class.getClassLoader().getResourceAsStream(resourcePath);
					SqlStatements statments = (SqlStatements) um.unmarshal(stream);
					
					Collection<SqlStatement> stmts = statments.getStatementList();
					Map<String, String> stmtsMap = new HashMap<String, String>();
					for (SqlStatement stmt : stmts)
					{
						stmtsMap.put(stmt.getName(), stmt.getValue());
					}
					logger.debug(String.format("%1$s contains %2$d statements", resourcePath, stmts.size()));
					
					cachedStatements.put(resourcePath, stmtsMap);
				}
			}
		}
	}
}

