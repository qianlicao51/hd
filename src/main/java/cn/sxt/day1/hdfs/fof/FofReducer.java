package cn.sxt.day1.hdfs.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FofReducer extends Reducer<Text, IntWritable, Text, Text> {
	Text rval = new Text();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> val, Context context)
			throws IOException, InterruptedException {

		// 对 key 求和
		// hadoop:hello 1
		// hadoop:hello 0
		// hadoop:hello 1
		// hadoop:hello 1
		int sum = 0;
		// 是否是直接好友,因为存在这种情况，他俩是直接好友，但是他俩同时也是别人的间接好友，这种情况排除
		boolean flag = true;

		for (IntWritable v : val) {
			if (v.get() == 0) {
				flag = false;
			}
			sum += v.get();
		}
		// 如果不是直接好友输出
		if (flag) {
			rval.set(sum + "");
			context.write(key, rval);
		}
	}
}
