package cn.zhuzi.mr.flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author 作者 grq
 * @version 创建时间：2018年6月30日 下午2:34:43
 *
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	FlowBean v = new FlowBean();
	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
		// 或许一行内容
		String line = value.toString();

		// 切割数据
		String[] split = line.split("\t");
		// 封装对象
		String phone = split[1];
		long upFlow = Long.parseLong(split[split.length - 3]);
		long downFlow = Long.parseLong(split[split.length - 2]);

		// 添加数据
		v.set(upFlow, downFlow);
		k.set(phone);
		// 输出
		context.write(k, v);
	}
}
