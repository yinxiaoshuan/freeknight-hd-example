
package org.freeknight.hadoop.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 处理左右表数据.
 * 
 * @author yrj
 *
 */
public class ReduceSideLeftRightJoinMapper
		extends Mapper< LongWritable, Text, Text, Text >
{

	/*
	 * 
	 * map方法的第一个参数为LongWritable, 该值代表待处理文本中一行的起始偏移量, 不能为IntWritable.
	 */
	@Override
	protected void map (
			LongWritable key, Text value, Mapper< LongWritable, Text, Text, Text >.Context context ) throws IOException, InterruptedException
	{
		// 起始偏移量为0, 代表第一行表头, 不做处理.
		if ( key.get ( ) == 0 )
		{
			return;
		}
		String[ ] ls = value.toString ( ).split ( ":" );

		String mapKey = null;
		String mapValue = null;
		char rel = 0;
		for ( int index = 0, length = ls.length; index < length; index++ )
		{
			// 判断当前下标是否为左右表的关联字段, 并根据关联字段的下标值index判断当前记录是左表还是右表(左表关联字段在左, 右表关联字段在右).
			char c = ls [ index ].charAt ( 0 );
			if ( c >= '0' && c <= '9' )// 用数字类型值作为关联字段. 更准确的判断为整个下标值匹配, 采用第一个字符匹配存在漏洞.
			{
				mapKey = ls [ index ];// 关联字段作为Mapper的输出Key.
				rel = index == 0 ? 'r' : 'l';// 下标为0, 代表右表; 否则代表左表.
				continue;
			}

			mapValue = ls [ index ];
		}

		// 关联字段为输出Key
		// 标记符 + 内容为输出Value, Reducer任务将截取Value的第一个字符判断当前内容为左表还是右表.
		context.write ( new Text ( mapKey ), new Text ( rel + mapValue ) );
	}
}
