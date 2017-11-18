
package org.freeknight.hadoop.multiple.input;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 论坛主题名、地址及数量统计Mapper
 * 
 * @author yrj
 *
 */
public class TopicMultipleInputSingleOutputMapper
		extends Mapper< Object, Text, Text, IntWritable >
{

	private final Pattern	TOPIC		= Pattern.compile ( "forum:(\\s+)(.*?),(\\s*)" );

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

	/**
	 * setup方法在Mapper生命周期内只执行一次.
	 * <p/>
	 * 注意：Mapper初始化是对应于输入目录下的文件, 一个文件对应一个Mapper实例. 换句话说, 有几个文件该方法会被调用几次.
	 */
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
		Matcher topicMatcher = TOPIC.matcher ( logLine );
		if ( !topicMatcher.find ( start ) )
		{
			return;
		}
		start = topicMatcher.end ( topicMatcher.groupCount ( ) );

		String singer = topicMatcher.group ( 2 );
		Matcher totalMatcher = TOTAL.matcher ( logLine );
		if ( !totalMatcher.find ( start ) )
		{
			return;
		}
		String total = totalMatcher.group ( 2 );
		int t = Integer.parseInt ( total );

		context.write ( new Text ( prefix + singer ), new IntWritable ( t ) );
	}

	/**
	 * 清除当前上下文持有的资源.
	 */
	@Override
	protected void cleanup (
			Mapper< Object, Text, Text, IntWritable >.Context context ) throws IOException, InterruptedException
	{
		super.cleanup ( context );
	}
}
