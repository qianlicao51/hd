package cn.sxt.day1.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	// 相同的key为一组 ，调用 一次reduce 进行计算
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : value) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}

}
