
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
public class SingerMultipleInputSingleOutputMapper
		extends Mapper< Object, Text, Text, IntWritable >
{

	private final Pattern	SINGER	= Pattern.compile ( "singer:(\\s*)(.*?),(\\s*)" );

	private final Pattern	TOTAL		= Pattern.compile ( "total:(\\s+)([0-9]+)" );

	/**
	 * <p>
	 * 单文件场景下, 此值为空串.
	 * </p>
	 * <p>
	 * 在多文件输出场景下, Reducer无法感知到来源Mapper, 也无法感知到即将写入的具体目标文件. 需要业务程序处理, 此处为了简单起见, 追加前缀prefix用于多文件输出确定具体的目标文件.
	 * </p>
	 * 
	 */
	private String				prefix	= "";

	public void setPrefix (
			String prefix )
	{
		this.prefix = prefix;
	}

	@Override
	protected void setup (
			Mapper< Object, Text, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		super.setup ( context );
	}

	@Override
	protected void map (
			Object key, Text value, Mapper< Object, Text, Text, IntWritable >.Context context ) throws IOException, InterruptedException
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
		String total = totalMatcher.group ( 2 );
		int t = Integer.parseInt ( total );
		context.write ( new Text ( prefix + singer ), new IntWritable ( t ) );
	}

	@Override
	protected void cleanup (
			Mapper< Object, Text, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		super.cleanup ( context );
	}
}
