package com.enhancebiz.service.common.db;

import java.util.Collection;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "statements")
@XmlAccessorType(XmlAccessType.FIELD)
public class SqlStatements
{
	@XmlElement(name = "statement")
	private Collection<SqlStatement> statementList;
	public Collection<SqlStatement> getStatementList()
	{
		return statementList;
	}

	public void setStatementList(Collection<SqlStatement> statementList)
	{
		this.statementList = statementList;
	}
}
