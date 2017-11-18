
package org.freeknight.hadoop.ssml;

import com.google.gson.Gson;

public class LogRecord
{

	private long		timeMillis;

	private String	thread;

	private String	level;

	private String	loggerName;

	private String	message;

	private Speak		speak;

	private boolean	lawful;

	public static LogRecord parse (
			String line )
	{
		Gson gson = new Gson ( );
		LogRecord record = gson.fromJson ( line, LogRecord.class );

		if ( record.getThread ( ).equals ( "main" ) )
		{
			return record;
		}
		String message = record.getMessage ( );
		if ( message == null || message.charAt ( 0 ) != '{' )
		{
			return record;
		}

		record.setLawful ( true );

		Speak speak = gson.fromJson ( message, Speak.class );
		record.setSpeak ( speak );
		return record;
	}

	@Override
	public String toString ( )
	{
		StringBuilder builder = new StringBuilder ( );
		builder.append ( "LogRecord [timeMillis=" );
		builder.append ( timeMillis );
		builder.append ( ", thread=" );
		builder.append ( thread );
		builder.append ( ", level=" );
		builder.append ( level );
		builder.append ( ", loggerName=" );
		builder.append ( loggerName );
		builder.append ( ", message=" );
		builder.append ( message );
		builder.append ( ", lawful=" );
		builder.append ( lawful );
		builder.append ( "]" );
		return builder.toString ( );
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

	public String getLevel ( )
	{
		return level;
	}

	public void setLevel (
			String level )
	{
		this.level = level;
	}

	public String getLoggerName ( )
	{
		return loggerName;
	}

	public void setLoggerName (
			String loggerName )
	{
		this.loggerName = loggerName;
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

	public boolean isLawful ( )
	{
		return lawful;
	}

	public void setLawful (
			boolean lawful )
	{
		this.lawful = lawful;
	}

}
