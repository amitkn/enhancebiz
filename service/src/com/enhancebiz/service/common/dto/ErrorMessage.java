package com.enhancebiz.service.common.dto;

import java.io.Serializable;

public interface ErrorMessage extends Serializable
{
	String getCode();
	String getMessage();
}
