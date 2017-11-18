
package org.freeknight.hadoop.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * MapReduce工作流.
 * 
 * @author yrj
 *
 */
public class Flow
{

	/**
	 * 迭代式.
	 * <p/>
	 * 上一个Job的输出作为下一个Job的输入, 只保留MapReduce任务的最终结果, 中间流程中产生的数据根据需求自行决定保留还是删除.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * 
	 */
	public void iteration ( ) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration ( );
		Job job_start = Job.getInstance ( conf, "start_job" );
		FileInputFormat.setInputPaths ( job_start, new Path ( "input_path" ) );
		FileOutputFormat.setOutputPath ( job_start, new Path ( "output_path_start" ) );
		job_start.waitForCompletion ( true );//  提交执行, 同步阻塞

		Job job_mid = Job.getInstance ( conf, "middle_job" );
		FileInputFormat.setInputPaths ( job_mid, FileOutputFormat.getOutputPath ( job_start ) );// 上一个Job的输出作为当前Job输入.
		FileOutputFormat.setOutputPath ( job_mid, new Path ( "output_path_mid" ) );
		job_start.waitForCompletion ( true );//  提交执行, 同步阻塞

		Job job_end = Job.getInstance ( conf, "end_job" );
		FileInputFormat.setInputPaths ( job_end, FileOutputFormat.getOutputPath ( job_mid ) );// 上一个Job的输出作为当前Job输入.
		FileOutputFormat.setOutputPath ( job_end, new Path ( "output_path_end" ) );
		job_start.waitForCompletion ( true );//  提交执行, 同步阻塞

	}

	/**
	 * 依赖控制.
	 * <p/>
	 * 通过JobControl作业运行图来组织, 通过加入作业配置告知JobControl作业之间的依赖关系.在一个线程中运行JobControl, 将按照依赖顺序执行作业.
	 * <p/>
	 * 作业结束后, 可以查看作业的状态和每个失败相关的错误信息. 如果一个作业失败, JobControl将不执行与之有依赖关系的后续作业.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void dependControl ( ) throws IOException, InterruptedException
	{
		Configuration conf = new Configuration ( );
		Job job_start = Job.getInstance ( conf, "start_job" );
		Job job_middle = Job.getInstance ( conf, "middle_job" );
		Job job_end = Job.getInstance ( conf, "end_job" );

		ControlledJob control_start = new ControlledJob ( conf );
		control_start.setJob ( job_start );
		ControlledJob control_middl = new ControlledJob ( conf );
		control_middl.setJob ( job_middle );
		ControlledJob control_end = new ControlledJob ( conf );
		control_end.setJob ( job_end );

		// 设置Job之间的依赖关系
		control_middl.addDependingJob ( control_start );// 依赖第一个Job
		control_end.addDependingJob ( control_middl );// 依赖第二个Job

		JobControl jobControl = new JobControl ( "ssml_log_job_group" );
		jobControl.addJob ( control_start );
		jobControl.addJob ( control_middl );
		jobControl.addJob ( control_end );

		new Thread ( jobControl ).start ( );

		while ( true )
		{
			if ( jobControl.allFinished ( ) )
			{
				jobControl.stop ( );// Stop的用途是啥.
				break;
			}
			Thread.sleep ( 100 );
		}
	}

	/**
	 * 线性链式.
	 * <p/>
	 * 使用场景：大量的数据处理任务涉及到对记录的预处理和后处理.
	 * <p/>
	 * 例如：处理信息检索文档时, 可能需要移除停用词（stop words）, 另一步做stemming处理(转换一个词的不同形式为相同形式, 例如finishing和finished为finish).
	 * <p/>
	 * 通过链式作业处理思路, 多个Mapper预先处理所有预处理步骤, 再让Reducer调用所有的后处理步骤.
	 * <p/>
	 * Hadoop提供了链式ChainMapper和ChainReducer来处理线性链式Job任务. 在Mapper或Reduce阶段存在多个Mapper, 这些Mapper像Linux管道一样,
	 * 前一个Mapper的输出结果直接重定向到后一个Mapper的输入, 形成流水线.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void linked ( ) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration config = new Configuration ( );
		Job job = Job.getInstance ( config, "chain_job" );
		job.setInputFormatClass ( TextInputFormat.class );
		job.setOutputFormatClass ( TextOutputFormat.class );

		FileInputFormat.addInputPath ( job, new Path ( "input_path" ) );
		FileOutputFormat.setOutputPath ( job, new Path ( "output_path" ) );

		/*
		 * 作业中添加Mapper.
		 */
		Configuration mapper_conf_1 = new Configuration ( );
		ChainMapper.addMapper ( job, Mapper.class, LongWritable.class, Text.class, Text.class, Text.class, mapper_conf_1 );

		Configuration mapper_conf_2 = new Configuration ( );
		ChainMapper.addMapper ( job, Mapper.class, Text.class, Text.class, LongWritable.class, Text.class, mapper_conf_2 );

		// 一个链式Job只能有一个Reducer阶段.
		Configuration reducer_conf = new Configuration ( );
		ChainReducer.setReducer ( job, Reducer.class, LongWritable.class, Text.class, Text.class, Text.class, reducer_conf );

		// 在Reducer之后, 通过添加Mapper做后处理.
		Configuration mapper_conf_3 = new Configuration ( );
		ChainReducer.addMapper ( job, Mapper.class, Text.class, Text.class, LongWritable.class, Text.class, mapper_conf_3 );

		job.waitForCompletion ( true );
	}
}
