
package org.freeknight.hadoop.flow.foreach;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自带排序功能, 按线程执行总数排序. 默认文本按ASCII码排序.
 * 
 * @author yrj
 *
 */
public class ThreadKeyComparable
		implements WritableComparable< ThreadKeyComparable >
{

	private String	thread;

	private int			count;

	@Override
	public void readFields (
			DataInput arg0 ) throws IOException
	{
		this.thread = arg0.readUTF ( );
		this.count = arg0.readInt ( );
	}

	@Override
	public void write (
			DataOutput arg0 ) throws IOException
	{
		arg0.writeUTF ( thread );
		arg0.writeInt ( count );
	}

	/*
	 * 在Mapper输出排序过程中, 由MapReduce自行调用该方法进行排序.
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo (
			ThreadKeyComparable o )
	{
		// 降序排序
		return o.getCount ( ) - this.count;
	}

	public String getThread ( )
	{
		return thread;
	}

	public void setThread (
			String thread )
	{
		this.thread = thread;
	}

	public int getCount ( )
	{
		return count;
	}

	public void setCount (
			int count )
	{
		this.count = count;
	}

	@Override
	public String toString ( )
	{
		return thread + ":" + count;
	}
}
