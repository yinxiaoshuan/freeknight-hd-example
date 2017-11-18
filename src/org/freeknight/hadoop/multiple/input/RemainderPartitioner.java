
package org.freeknight.hadoop.multiple.input;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 按值对分区数量取余数的分区类.
 * 
 * @author yrj
 *
 */
public class RemainderPartitioner
		extends HashPartitioner< MulMulKeyPair, IntWritable >
{

	@Override
	public int getPartition (
			MulMulKeyPair key, IntWritable value, int numReduceTasks )
	{
		// numReduceTasks, 由JobContext设置的值.
		return ( key.hashCode ( ) / 127 ) % numReduceTasks; // 依据Value值对分区数量取余计算数据分配到的分区.
		// return super.getPartition ( key, value, numReduceTasks );
	}
}
