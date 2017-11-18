
package org.freeknight.hadoop.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideLeftRightJoinReducer
		extends Reducer< Text, Text, Text, Text >
{

	@Override
	protected void reduce (
			Text key, Iterable< Text > values, Reducer< Text, Text, Text, Text >.Context context ) throws IOException, InterruptedException
	{
		/*
		 * 分析: 在Mapper、Reducer任务流程中, 有一个讨巧的思路: MapKey为关联字段. 这样确保了不管是左表还是右表, 统一发送到同一个Reducer任务, 并将一并作为Iterable发送过来. 内部根据左右表标记符判断.
		 */
		List< String > lefts = new ArrayList< String > ( );
		List< String > rights = new ArrayList< String > ( );

		// 根据values中第一个字节char等于r还是l, 解析出左右表并分别存储.
		for ( Text value : values )
		{
			char c = value.toString ( ).charAt ( 0 );
			if ( c == 'l' )// left, 左表.
			{
				lefts.add ( value.toString ( ).substring ( 1 ) );
			}
			if ( c == 'r' )// right, 右表.
			{
				rights.add ( value.toString ( ).substring ( 1 ) );
			}
		}

		// 笛卡尔积
		for ( String left : lefts )
		{
			for ( String right : rights )
			{
				context.write ( new Text ( left ), new Text ( right ) );
			}
		}

	}
}
