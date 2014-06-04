package com.enhancebiz.service.common.exception;

import com.enhancebiz.service.common.dto.ErrorMessage;
import com.enhancebiz.service.common.dto.ServiceException;


public class ExceptionUtil
{
	public static ServiceException generateServiceException(ErrorMessage message, Throwable e)
	{
		ServiceException excep;
		if (e instanceof ServiceException)
		{
			excep = (ServiceException) e;
		}
		else
		{
			excep = new ServiceException(message, e);
		}
		return excep;
	}
	
	public static ServiceException generateServiceException(ErrorMessage message)
	{
		return new ServiceException(message, null);
	}	
}
