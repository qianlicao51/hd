package cn.zhuzi.mr.flow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author 作者 grq
 * @version 创建时间：2018年6月30日 下午2:44:24
 *
 */
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
	FlowBean v = new FlowBean();

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

		// 汇总
		long sum_up = 0;
		long sum_down = 0;

		for (FlowBean flowBean : values) {
			sum_up += flowBean.getUpFlow();
			sum_down += flowBean.getDownFlow();
		}
		v.set(sum_up, sum_down);
		// 输出
		context.write(key, v);
	}
}
