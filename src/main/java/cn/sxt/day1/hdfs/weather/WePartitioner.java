package cn.sxt.day1.hdfs.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分组
 * 
 * @author MI
 *
 */
public class WePartitioner extends Partitioner<Weath, IntWritable> {

	@Override
	public int getPartition(Weath key, IntWritable value, int numPartitions) {

		return key.hashCode() % numPartitions;
	}

}
