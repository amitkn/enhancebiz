package com.enhancebiz.test;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.enhancebiz.service.common.conf.ConfigManager;
import com.enhancebiz.service.common.conf.DatabaseInfo;
import com.enhancebiz.service.common.db.QueryExecutor;

public class SQLStatementTest
{
	private static final String SQL_XML_PATH = "com/spoken/datamgr/common/sql/TestSql.xml";
	
	@Before
	public void setup()
	{
	}

	@Test
	public void testInsertStatement() throws Exception
	{
		DatabaseInfo database = ConfigManager.getDatabase("medical");
		QueryExecutor executor = new QueryExecutor(database, SQL_XML_PATH);
		String sql = executor.getSql("test1");
		Assert.assertTrue(sql != null);
	}

}
