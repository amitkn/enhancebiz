package com.enhancebiz.service.common.db;

import com.enhancebiz.service.common.exception.ErrorMessageImpl;

public interface SqlError 
{
	ErrorMessageImpl QUERY_EXECUTOR_NO_FIELDS = new ErrorMessageImpl("query_executor_has_no_fields", "Query Executor result class has no fields");
	ErrorMessageImpl QUERY_EXECUTOR_FAILED = new ErrorMessageImpl("query_executor_failed", "Query Executor failed with an exception");
	ErrorMessageImpl QUERY_EXECUTOR_NO_ANNOTATIONS = new ErrorMessageImpl("query_executor_has_no_annotations", "Query Executor result class has no annontations");
	
	ErrorMessageImpl STMT_READER_PARSE_FAILED = new ErrorMessageImpl("stmt_reader_parse_failed", "Sql Statement Parse failed from Statement Reader");
	ErrorMessageImpl STMT_READER_NOT_FOUND = new ErrorMessageImpl("stmt_reader_statement_not_found", "Sql Statement could not be found");
}
