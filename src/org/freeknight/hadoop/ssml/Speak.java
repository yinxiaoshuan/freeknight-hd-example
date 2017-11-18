
package org.freeknight.hadoop.ssml;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Speak
{
	private static Pattern	pattern	= Pattern.compile ( "speechText\":\"(.*)\"," );

	private String					remote_host;

	private int							timeuse;

	private String					args;

	public String getRemote_host ( )
	{
		return remote_host;
	}

	public void setRemote_host (
			String remote_host )
	{
		this.remote_host = remote_host;
	}

	public int getTimeuse ( )
	{
		return timeuse;
	}

	public void setTimeuse (
			int timeuse )
	{
		this.timeuse = timeuse;
	}

	public String getArgs ( )
	{
		return args;
	}

	public void setArgs (
			String args )
	{
		this.args = args;
	}

	public String getSpeaker ( )
	{
		String speak = null;
		Matcher matcher = pattern.matcher ( this.args );
		if ( matcher.find ( ) )
		{
			speak = matcher.group ( 1 );
		}
		return speak;
	}
}
