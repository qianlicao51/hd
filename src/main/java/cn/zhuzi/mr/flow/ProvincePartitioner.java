package cn.zhuzi.mr.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author 作者 grq
 * @version 创建时间：2018年6月30日 下午3:43:37
 *
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		int part = 4;

		String preNum = key.toString().substring(0, 3);
		if ("136".equals(preNum)) {
			part = 0;
		} else if ("135".equals(preNum)) {
			part = 1;
		} else if ("139".equals(preNum)) {
			part = 2;
		}

		return part;
	}

}
