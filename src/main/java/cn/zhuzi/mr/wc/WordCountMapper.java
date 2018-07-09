package cn.zhuzi.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author 作者 grq
 * @version 创建时间：2018年6月29日 下午3:24:16 KEYIN map的输入 行号 VALUEIN, map的value输入 text
 *          一行内容 , KEYOUT map 的 输出 Text 单词 VALUEOUT map 的输出 单词个数
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text k = new Text();
	IntWritable v = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// 1 获取一行数据
		String line = value.toString();
		// 2 切割
		String[] words = line.split(" ");
		// 3 发送数据
		for (String str : words) {
			k.set(str);
			context.write(k, v);
		}
	}
}
