
package org.freeknight.hadoop.flow;

import org.freeknight.framework.helper.GsonConverter;

public class Log
{

	private final static String	UNVALID_THREAD	= "admin";

	private long								timeMillis;

	private String							thread;

	private String							message;

	private Speak								speak;

	private boolean							valid;											// 默认非法.

	public static Log doParse (
			String logLine )
	{
		Log log = GsonConverter.toBean ( logLine, Log.class );
		if ( log == null || UNVALID_THREAD.equals ( log.getThread ( ) ) )
		{
			return log;
		}
		String message = log.getMessage ( );
		if ( message == null || message.charAt ( 0 ) != '{' )
		{
			return log;
		}

		Speak speak = Speak.doParse ( message );
		log.setSpeak ( speak );

		log.setValid ( true );
		log.setMessage ( "" );// JSON数据, 已无存在意义, 删除.
		return log;
	}

	public long getTimeMillis ( )
	{
		return timeMillis;
	}

	public void setTimeMillis (
			long timeMillis )
	{
		this.timeMillis = timeMillis;
	}

	public String getThread ( )
	{
		return thread;
	}

	public void setThread (
			String thread )
	{
		this.thread = thread;
	}

	public String getMessage ( )
	{
		return message;
	}

	public void setMessage (
			String message )
	{
		this.message = message;
	}

	public Speak getSpeak ( )
	{
		return speak;
	}

	public void setSpeak (
			Speak speak )
	{
		this.speak = speak;
	}

	public boolean isValid ( )
	{
		return valid;
	}

	public void setValid (
			boolean valid )
	{
		this.valid = valid;
	}

	@Override
	public String toString ( )
	{
		return GsonConverter.toJson ( this );
	}
}
