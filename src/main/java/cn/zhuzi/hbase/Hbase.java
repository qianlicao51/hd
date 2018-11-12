package cn.zhuzi.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @Title: Hbase.java
 * @Package cn.zhuzi.hbase
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年11月12日 上午9:24:27
 *
 */
public class Hbase {
	/**
	 * 创建 Hadoop以及hbase管理对象的配置文件
	 */
	public static Configuration conf;
	public static HBaseAdmin admin;
	static {
		// 使用HBaseConfiguration 单例方法实例化
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop");// 单机
		conf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口

		// TODO 实际操作中下面这个没设置，hbase是如何找到hadoop的？
		// conf.set("hbase.rootdir", "hdfs://ncst:9000/hbase");
		try {
			admin = new HBaseAdmin(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 向hbase中放入数据
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		putData();
		// getData();
	}

	/**
	 * 从hbase中取出数据
	 * 
	 * @throws IOException
	 */
	private static void getData() throws IOException {
		// 初始化一个新的表引用
		HTable hTable = new HTable(conf, "fruit");

		// 使用一个制定的行键 构建一个get实例
		Get get = new Get(Bytes.toBytes("row1"));

		// 向get中添加一个列
		Get addColumn = get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("qua"));
		// 从hbase中获取制定列的数据
		Result result = hTable.get(get);
		// 从返回的结果中获取对饮的列数据
		byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("qua"));
		System.out.println("values " + Bytes.toString(value));

	}

	private static void putData() throws IOException, InterruptedIOException, RetriesExhaustedWithDetailsException {
		// 实例化一个客户端
		HTable hTable = new HTable(conf, "fruit");
		// 制定一个行来创建一个put
		Put put = new Put(Bytes.toBytes("row2"));
		// 向put添加一个名为 qua:val1 的列
		Put add = put.add(Bytes.toBytes("info"), Bytes.toBytes("qua"), Bytes.toBytes("val1"));

		Put put1 = new Put(Bytes.toBytes("row2"));
		// 向put添加一个名为 qua:val1 的列
		Put add2 = put1.add(Bytes.toBytes("info"), Bytes.toBytes("qua2"), Bytes.toBytes("val2"));

		List<Put> puts = new ArrayList<Put>(2);
		puts.add(put1);
		puts.add(put);
		// 将这一行存储到hbase表中
		hTable.put(puts);
	}
}
