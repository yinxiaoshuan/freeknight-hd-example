
package org.freeknight.hadoop.join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Reduce端的左右表关联.
 * <p/>
 * 将左表和右表进行关联, 通过中间关联的ID匹配出右表信息.
 * 
 * <pre>
 * <strong>左表: 旅游景点对应城市ID</strong>
 * viewSpot:cityId (表头)
 * 天安门:1
 * 故宫:1
 * 黄埔江:2
 * 泰山:3
 * 布达拉宫:9
 * </pre>
 * 
 * <pre>
 * <strong>右表: 城市ID对应城市名</strong>
 * cityId:cityName (表头)
 * 1:北京
 * 2:上海
 * 3:山东
 * 4:广州
 * 9:拉萨
 * </pre>
 * 
 * <pre>
 * <strong>结果: 旅游景点对应城市名</strong>
 * viewSpot:cityName (表头)
 * 天安门:北京
 * 故宫:北京
 * 黄埔江:上海
 * 泰山:山东
 * 布达拉宫:拉萨
 * </pre>
 * 
 * @author yrj
 *
 */
public class ReduceSideJoinDriver
{

	public static void main (
			String[ ] args ) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration ( );
		Job join_job = Job.getInstance ( conf, ReduceSideJoinDriver.class.getCanonicalName ( ) + ".job" );
		join_job.setJarByClass ( ReduceSideJoinDriver.class );
		join_job.setOutputKeyClass ( Text.class );
		join_job.setOutputValueClass ( Text.class );
		// OutputKey/OutputValue 与 MapOutputKey/MapOutputValue 必须至少有一组需要指定类型, 或全部指定, 都不指定会抛出异常.
		// 具体对作业流程产生何种影响暂不理解.

		join_job.setMapperClass ( ReduceSideLeftRightJoinMapper.class );
		join_job.setReducerClass ( ReduceSideLeftRightJoinReducer.class );

		// join_job.setMapOutputKeyClass ( Text.class );
		// join_job.setMapOutputValueClass ( Text.class );

		String inputPath = "hdfs://192.168.1.100:9000/user/join";
		String outputPath = "hdfs://192.168.1.100:9000/user/join/output";
		FileInputFormat.addInputPath ( join_job, new Path ( inputPath ) );
		FileOutputFormat.setOutputPath ( join_job, new Path ( outputPath ) );

		join_job.waitForCompletion ( true );
	}
}
