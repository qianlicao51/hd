package cn.zhuzi.hbase.mr_hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Title: HDFSDriver.java
 * @Package cn.zhuzi.hbase.mr_hdfs
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年11月5日 下午7:17:42
 *
 */
public class HDFSDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		// 组装 JOB
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(HDFSDriver.class);

		// 输入路径
		Path inPath = new Path("hdfs://hadoop:9000//input/fruit.tsv");
		// 添加输入路径
		FileInputFormat.addInputPath(job, inPath);

		// 设置mapper
		job.setMapperClass(ReadDataFromHDFSMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		// 设置 Reduce
		TableMapReduceUtil.initTableReducerJob("fruits_hdfs", WriteHBaseReducer.class, job);
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
		int run = ToolRunner.run(conf, new HDFSDriver(), args);
		System.exit(run);

	}
}
