#MapReduce迭代、依赖及链式组合

	一些复杂的任务难以用一次MapReduce处理完成, 需要多个MapReduce按一定执行顺序配合完成作业, 从而完成一个复杂的任务输出.例如Pagrank、K-means算法都需要多次迭代完成.
	在这里将通过简单示例介绍MapReduce作业的三种组合方式, 便于掌握多MapReduce的多组合开发完成复杂任务.

##1. 迭代式MapReduce
	MapReduce的迭代思想类似foreach循环, 前一个MapReduce作业的输出作为下一个MapReduce作业的输入, 产生的中间作业输出可根据业务需要删除或保留下来.
	原始日志文件说明：
		原始日志为JSON格式的业务日志, 样板日志目录：doc/ssml-info-2017-11-03-1.log、doc/ssml-info-2017-11-08-1.log.
		每行代表一条业务日志输出, 有当前timestamp、thread、message等信息, 其中message中记录当前日志真实的业务信息输出.
		我们只关注message信息为JSON串的行, 注入time1、time2的行被丢弃.

	业务场景描述：
	按线程维度统计, 统计出不同线程每天使用的次数, 并输出到目标目录: hdfs://1.1.1.1:9000/ssml/output/thread/statistics
	
	Job流程分析：
	第一步：读取原始日志文件并解析出timestamp、thread、timeuse、remote_host、log_id、intent、speechText、spd信息, 并输出到新的输出日志中.
	格式：
	{
		"timestamp":1510109436200,
		"thread":"http-nio-10.14.6.21-8899-exec-1",
		"timeuse":40,
		"remote_host":"10.63.29.38",
		"log_id":"ec8a18c28f9c4651863347d9aea3e01c",
		"intent":"RenderCard",
		"speechText":"是否需要创建看球提醒？"
		"spd":3.12
	}
	
	第二步：分析第一步产生的输出, 按线程维度统计不同线程执行量并按升序排列, 输出到目标目录.
	格式：thread:${count}
	例如：
	http-nio-10.14.6.21-8899-exec-9:873
	http-nio-10.14.6.21-8899-exec-1:12
	
	第三步：为了更好地说明迭代式MapReduce, 加入第三步, 将top10线程及执行量写入到统计目录: hdfs://1.1.1.1:9000/ssml/output/thread/statistics
	格式：thread:${count}
	例如：
	http-nio-10.14.6.21-8899-exec-9:873
	http-nio-10.14.6.21-8899-exec-1:12
	
	代码示例
	关键点：
	1. 前一个作业输出作为下一个作业输入,目录需要匹配
	2. 前一个作业输出Key、Value与下一个作业输入Key、Value匹配.

##2. 依赖组合MapReduce
	当某一个负责任务由多个作业组成, 有的作业无依赖(可并行)、有的作业依赖其他作业执行结果情况. 采用"迭代式MapReduce"将不在合适.
	MapReduce提供JobControl类支持复杂依赖关系的Job作业.
	原始日志文件说明：同"迭代式MapReduce"相同
	
	业务场景描述：
	按访问IP和线程维度统计, 统计出不同线程每天处理不同IP的使用次数, 并输出到目录目录：hdfs://1.1.1.1:9000/ssml/output/thread_ip/statistics.
	
	Job流程分析：
	第一步：同"迭代式MapReduce"的第一步相同
	第二步：分析第一步作业的输出,按访问IP、线程维度统计

##3. 链式MapReduce
