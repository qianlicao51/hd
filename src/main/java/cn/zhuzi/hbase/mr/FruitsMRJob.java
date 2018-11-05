package cn.zhuzi.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Title: FruitsMRJob.java
 * @Package cn.zhuzi.hbase.mr
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年11月5日 下午12:06:04
 *          https://blog.csdn.net/ys_230014/article/details/83714141
 */
public class FruitsMRJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		// 组装 JOB
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(FruitsMRJob.class);

		// 配置JOB
		Scan scan = new Scan();
		scan.setCacheBlocks(false);
		scan.setCaching(500);

		// 设置Mapper，导入的包是mapreduce
		TableMapReduceUtil.initTableMapperJob("fruits", // 数据源的表名
				scan, // scan 扫描控制器
				ReadFruitsMapper.class,// 设置 Mapper 类
				ImmutableBytesWritable.class,// 设置 Mapper 输出 key 类型
				Put.class,// 设置 Mapper 输出 value 值类型
				job// 设置给哪个 JOB
				);

		// 设置 Reduce
		TableMapReduceUtil.initTableReducerJob("fruits_mr", WriteFruitsMRreducer.class, job);
		// 设置Reduce数量，最小是1个
		job.setNumReduceTasks(1);
		boolean completion = job.waitForCompletion(true);
		if (!completion) {
			throw new IOException(" JOB 运行错误");
		}
		return completion ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop");// 单机
		// zookeeper地址
		conf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		int run = ToolRunner.run(conf, new FruitsMRJob(), args);
		System.exit(run);

	}
}
