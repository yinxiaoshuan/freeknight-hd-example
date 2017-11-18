#自定义计数器
	Hadoop提供了多种内置计数器, 自定义计数器常适用于统计无效记录或统计敏感数据等信息.

	定义计数器有两种方式:
	1. 通过枚举类型定义
		public enum LineCounterEnum{
			TOTAL,ERROR;
		}
	2. 动态声明计数器, 不需要使用枚举.
		context.getCounter(String groupName, String counterName);
	
	这里描述通过属性资源文件方式配置计数器.
	第一步: 定义枚举Enum, 如上面的LineCounterEnum.
	第二步: 在枚举相同包目录下创建资源文件 LineCounterEnum.properties, 若当前枚举为某个类的内部类, 则资源文件名称为: OuterClass_LineCounterEnum.properties
	第三步: 资源文件中定义该计数器的组名groupName和计数器名称.
		#
		# 声明GroupName, MapReduce任务执行完毕会在控制台输出.
		#
		CounterGroupName=Custom Counter Group
		#
		# 指定每个计数器的名称, 若不指定则不会在控制台输出.
		# TOTAL.name 指定枚举字段TOTAL的计数器名称;
		# ERROR.name 指定枚举字段ERROR的计数器名称.
		#
		TOTAL.name=TOTAL-Line
		ERROR.namme=ERROR-Line
		注意: 不论组名还是计数器名称, 若指定中文会出现乱码输出情况. 暂无解决办法(也不排除跟本地环境有关).
	
	计数器的一种使用方式:
	1. Mapper中为计数器赋值
	public class OrderByMapper extends Mapper< LongWritable, Text, DoubleWritable, Text >
		@Override
		protected void map (
				LongWritable key, Text value, Mapper< LongWritable, Text, DoubleWritable, Text >.Context context ) throws IOException, InterruptedException
		{
			context.getCounter ( LineCounter.TOTAL ).increment ( 1 );//总数, 无论对错.

			String[ ] items = value.toString ( ).split ( "\t" );
			if ( items == null || items.length != 2 )
			{//数据异常、非法情况, 记录到ERROR_LINE计数器.
				context.getCounter ( LineCounter.ERROR_LINE ).increment ( 1 );
				return;
			}

			context.write ( new DoubleWritable ( Double.parseDouble ( items [ 2 ] ) ), value );
		}
	}
	
	2. 作业执行完毕, 根据计数器值执行不同逻辑处理.
	boolean compled = job.waitForCompletion ( true );
	if ( compled )
	{
		// 后续逻辑处理视业务而定, 比如 ERROR_LINE 有值, 则重试、采取补救错误等.
		long total = job.getCounters ( ).findCounter ( LineCounter.TOTAL ).getValue ( );
		long error = job.getCounters ( ).findCounter ( LineCounter.ERROR_LINE ).getValue ( );
		System.out.println ( "Total: " + total + ", error: " + error );
		if(error > 0){
			System.out.println("My God, fuck. 赶紧做点事情吧.");
		}
	}
		