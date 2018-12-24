package cn.sxt.day1.hdfs.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FofMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text mkey = new Text();
	IntWritable mval = new IntWritable();

@Override
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	// 数据 格式是 tom hello hadoop cat(每个人和他的好友以空格分隔)
	String[] split = StringUtils.split(value.toString(), ' ');
	String user = split[0];

	for (int i = 1; i < split.length; i++) {
		mkey.set(friends(user, split[i]));
		mval.set(0);
		context.write(mkey, mval);
		System.out.println(mkey.toString() + mval);
		// 如果 A 和B 是直接 好友就输出 A:B 0
		for (int j = i + 1; j < split.length; j++) {
			mkey.set(friends(split[j], split[i]));
			mval.set(1);
			// 如果 A 和B 是间接 好友就输出 A:B 1；然后对key分组就和 和就是他们有多少共同好友
			context.write(mkey, mval);
			System.out.println(mkey.toString() + mval);
		}
	}

}

	public static String friends(String friendA, String friendB) {
		if (friendA.compareTo(friendB) > 0) {
			return friendA + ":" + friendB;
		}
		return friendB + ":" + friendA;
	}
}
