package cn.zhuzi.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author 作者 grq
 * @version 创建时间：2018年6月29日 下午3:59:48 EYIN, VALUEIN, KEYOUT, VALUEOUT
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable result = new IntWritable();

	@Override
	protected void reduce(Text text, Iterable<IntWritable> value, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// 1 汇总
		int sum = 0;
		for (IntWritable count : value) {
			sum += count.get();
		}
		
		result.set(sum);
		// 2输出
		context.write(text, result);
	}
}
