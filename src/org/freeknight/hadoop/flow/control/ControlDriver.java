
package org.freeknight.hadoop.flow.control;

import org.apache.hadoop.conf.Configuration;

/**
 * 依赖控制MapReduce示例.
 * 
 * @author yrj
 *
 */
public class ControlDriver
{

	public static void main (
			String[ ] args )
	{
		Configuration conf = new Configuration ( );

		String inputPath = "hdfs://192.168.1.100:9000/user/ssml/ssml-info-2017-11-03-1.log";
		String outputPath = "hdfs://192.168.1.100:9000/user/ssml/control";

	}
}
