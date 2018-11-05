package cn.zhuzi.hbase.mr_hdfs;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Title: ReadDataFromHDFSMapper.java
 * @Package cn.zhuzi.hbase.mr_hdfs
 * @Description: TODO(读取文本数据到HBASE)
 * @author 作者 grq
 * @version 创建时间：2018年11月5日 下午7:02:17
 *
 */
public class ReadDataFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {

		String line = value.toString();
		// 倒数数据的同时清洗数据
		String[] values = line.split("\t");
		String row = values[0];
		String name = values[1];
		String infos = values[2];

		//初始化rowKey
		ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(row.getBytes());
		//初始化put
		Put put = new Put(Bytes.toBytes(row));
		//参数分别是 列族、列、值
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(infos));

		context.write(immutableBytesWritable, put);
	}
}
