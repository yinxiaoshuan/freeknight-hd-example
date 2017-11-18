
package org.freeknight.hadoop.multiple.input;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 歌手、歌曲列表及数量Mapper.
 * 
 * @author yrj
 *
 */
public class SingerMultipleInputMultipleOutputMapper
		extends Mapper< Object, Text, MulMulKeyPair, IntWritable >
{

	private final Pattern				SINGER				= Pattern.compile ( "singer:(\\s*)(.*?),(\\s*)" );

	private final Pattern				TOTAL					= Pattern.compile ( "total:(\\s+)([0-9]+)" );

	public final static String	SINGER_NAMED	= "singer";

	@Override
	protected void map (
			Object key, Text value, Mapper< Object, Text, MulMulKeyPair, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		String logLine = value.toString ( );

		int start = 0;
		Matcher singerMatcher = SINGER.matcher ( logLine );
		if ( !singerMatcher.find ( start ) )
		{
			return;
		}
		start = singerMatcher.end ( singerMatcher.groupCount ( ) );

		String singer = singerMatcher.group ( 2 );
		Matcher totalMatcher = TOTAL.matcher ( logLine );
		if ( !totalMatcher.find ( start ) )
		{
			return;
		}
		String totalStr = totalMatcher.group ( 2 );
		int total = Integer.parseInt ( totalStr );

		context.write ( new MulMulKeyPair ( SINGER_NAMED, singer, total ), new IntWritable ( total ) );
	}

}
