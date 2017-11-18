#MapReduce多路径输入输出

样板文件: ip-hosts.txt

##多输出路径
	默认情况下, MapReduce默认采用part-*作为输出文件前缀. 利用MultipleOutputs实现输出自定义文件名.
	MultipleOutputs还可以将不同的键值对输出到自定义的不同文件中.
	通过 new MultipleOutputs().write ( key, value, baseOutputPath ) 方法, 在第三个参数中指定输出文件前缀. 我们可以通过对不同的key使用不同的baseOutputPath来使不同key对应的value输出到不同的文件中，比如将同一天的数据输出到以该日期命名的文件中.
	
	示例:

##分区处理
	在进程MapReduce计算时, 有时需要把不同数据输出不同文件, 例如按手机号段、省份、性别等划分数据到不同文件.
	数据的最终写入HDFS文件由Reduce任务输出, 如果要得到多个拆分文件, 则需要有多个Reduce任务运行. Reduce任务数据来源Mapper任务, Mapper任务负责数据划分并分配给不同Reduce任务.
	数据的拆分类为Partitioner类, 通过调用getPartition方法根据Mapper的输出Key、Value及分区数决定数据写入哪个分区.
	public int getPartition ( K key, V value, int numReduceTasks );
	K: 对应Mapper的输出Key数据类型, V: 对应Mapper 的输出Value数据类型, numReduceTasks: 分区数量.
	
	设置分区数量及分区类:
	Job job = Job.getInstance(Configuration, String);
	job.setNumReduceTasks(int numReduceTasks)
	job.setPartitionerClass ( RemainderPartitioner.class );//设置自定义的分区类.
	
	/**
	 * 按值对分区数量取余数的分区类.
	 * 
	 * @author yrj
	 *
	 */
	public class RemainderPartitioner
			extends HashPartitioner< Text, IntWritable >
	{
	
		@Override
		public int getPartition (
				Text key, IntWritable value, int numReduceTasks )
		{
			int count = value.get ( );
			return count % numReduceTasks; // 依据Value值对分区数量取余计算数据分配到的分区.
			// return super.getPartition ( key, value, numReduceTasks );
		}
	}
	
	输出结果:
	根据以上配置, 设置分区数量为: 3, 则默认产生的分区文件: part-r-00000、part-r-00001、part-r-00002.
	