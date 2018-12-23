package cn.sxt.day1.hdfs.wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text wd = new Text();

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString());
		while (tokenizer.hasMoreElements()) {
			wd.set(tokenizer.nextToken());
			context.write(wd, one);
		}
	}

}
