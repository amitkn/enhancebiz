package com.enhancebiz.service.common.dto;


public class ServiceException extends Exception
{
	private static final long serialVersionUID = 1L;

	private Throwable exception;
	private String causedBy;
	private String code;
	private String message;
	
	public ServiceException()
	{
	}
	
	public ServiceException(ErrorMessage msg, Throwable e)
	{
		super(msg.getMessage());
		this.code = msg.getCode();
		this.message = msg.getMessage();
		if (e != null)
		{
			causedBy = e.getMessage();
			exception = e;
		}
	}

	public String getCode()
	{
		return code;
	}

	public String getMessage()
	{
		return message;
	}

	public String getCausedBy()
	{
		return causedBy;
	}

	public Throwable getException()
	{
		return exception;
	}
}
