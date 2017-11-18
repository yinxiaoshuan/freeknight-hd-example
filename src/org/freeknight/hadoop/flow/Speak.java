
package org.freeknight.hadoop.flow;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.freeknight.framework.helper.GsonConverter;

public class Speak
{

	private static Pattern	intentPattern	= Pattern.compile ( "\\\\\"intent\\\\\":\\\\\"(.*)\\\\\",\\\\\"queryText\\\\\"" );

	private static Pattern	spdPattern		= Pattern.compile ( "\\\\\"spd\\\\\":\\\\\"(.*)\\\\\"}}}]" );

	private static Pattern	speechPattern	= Pattern.compile ( "<speak>(.*)</speak>" );

	private String					remote_host;

	private String					intent;

	private String					speechText;

	private double					spd;

	private String					log_id;

	private int							timeuse;

	public static Speak doParse (
			String message )
	{
		Speak speak = GsonConverter.toBean ( message, Speak.class );
		Matcher intentMatcher = intentPattern.matcher ( message );

		if ( intentMatcher.find ( ) )
		{
			speak.setIntent ( intentMatcher.group ( 1 ) );
		}

		Matcher spdMatcher = spdPattern.matcher ( message );
		if ( spdMatcher.find ( ) )
		{
			speak.setSpd ( Double.parseDouble ( spdMatcher.group ( 1 ) ) );
		}

		Matcher speechMatcher = speechPattern.matcher ( message );
		if ( speechMatcher.find ( ) )
		{
			speak.setSpeechText ( speechMatcher.group ( 1 ) );
		}
		return speak;
	}

	public String getRemote_host ( )
	{
		return remote_host;
	}

	public void setRemote_host (
			String remote_host )
	{
		this.remote_host = remote_host;
	}

	public String getIntent ( )
	{
		return intent;
	}

	public void setIntent (
			String intent )
	{
		this.intent = intent;
	}

	public String getSpeechText ( )
	{
		return speechText;
	}

	public void setSpeechText (
			String speechText )
	{
		this.speechText = speechText;
	}

	public double getSpd ( )
	{
		return spd;
	}

	public void setSpd (
			double spd )
	{
		this.spd = spd;
	}

	public String getLog_id ( )
	{
		return log_id;
	}

	public void setLog_id (
			String log_id )
	{
		this.log_id = log_id;
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

	@Override
	public String toString ( )
	{
		StringBuilder builder = new StringBuilder ( );
		builder.append ( "Speak [remote_host=" );
		builder.append ( remote_host );
		builder.append ( ", intent=" );
		builder.append ( intent );
		builder.append ( ", speechText=" );
		builder.append ( speechText );
		builder.append ( ", spd=" );
		builder.append ( spd );
		builder.append ( ", log_id=" );
		builder.append ( log_id );
		builder.append ( ", timeuse=" );
		builder.append ( timeuse );
		builder.append ( "]" );
		return builder.toString ( );
	}
}
