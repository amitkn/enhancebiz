package com.enhancebiz.service.common.exception;

import com.enhancebiz.service.common.dto.ErrorMessage;


public class ErrorMessageImpl implements ErrorMessage
{
	private static final long	serialVersionUID	= 1L;
	
	private String code;
	private String message;
	
	protected ErrorMessageImpl()
	{
	}
	
	public ErrorMessageImpl(String code, String message)
	{
		this.code = code;
		this.message = message;
	}

	public String getCode()
	{
		return code;
	}

	public String getMessage()
	{
		return message;
	}
	
	public ErrorMessage format(Object... params)
	{
		String formattedMsg = message;
//		if (params.length > 0)
//			formattedMsg = String.format(message, params);
		return new ErrorMessageImpl(code, formattedMsg);
	}
}

