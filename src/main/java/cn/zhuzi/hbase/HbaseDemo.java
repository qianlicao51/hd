package cn.zhuzi.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @Title: HbaseDemo.java
 * @Package cn.zhuzi.hbase
 * @Description: TODO(hbase demo)
 * @author 作者 grq
 * @version 创建时间：2018年10月28日 下午3:16:07
 *
 */
public class HbaseDemo {

	/**
	 * 创建 Hadoop以及hbase管理对象的配置文件
	 */
	public static Configuration conf;
	public static HBaseAdmin admin;
	static {
		// 使用HBaseConfiguration 单例方法实例化
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop");// 单机
		// conf.set("hbase.zookeeper.quorum", "master,work1,work2");//
		// zookeeper地址
		conf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		try {
			admin = new HBaseAdmin(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 创建表
	 * 
	 * @param tableName
	 *            表名
	 * @param columnFamily
	 *            列族
	 */
	public static void createTable(String tableName, String... columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		if (isTableExists(tableName)) {
			System.out.println("table is exists");
		} else {
			// 创建表属性对象，表明需要转为字节
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			// 创建多个列族
			for (String cf : columnFamily) {
				descriptor.addFamily(new HColumnDescriptor(cf));
			}
			// 根据配置创建表
			admin.createTable(descriptor);
		}
	}

	/**
	 * 判断表是否存在
	 * 
	 * @param tableName
	 *            表名
	 * @return
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws IOException
	 */
	public static boolean isTableExists(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		// 在Hbase中管理、访问表需要先创建 hbaseadmin对象
		return admin.tableExists(tableName);

	}

	public static void dropTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		if (isTableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("HbaseDemo.dropTable() is success");
		} else {
			System.out.println("table is not exists。");
		}

	}

	/**
	 * 插入数据
	 * 
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param column
	 * @param value
	 * @throws IOException
	 */
	public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
		HTable hTable = new HTable(conf, tableName);
		Put put = new Put(rowKey.getBytes());
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		hTable.put(put);
		hTable.close();
	}

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		boolean tableExists = isTableExists("stu");
		// System.out.println("HbaseDemo.main()" + tableExists);
		// createTable("person", "base_info", "job", "heathy");
		// dropTable("pser");
		addRowData("person", "001", "base_info", "name", "dou");
		addRowData("person", "001", "base_info", "age", "25");
		addRowData("person", "001", "base_info", "sex", "Male");
		addRowData("person", "001", "job", "dept_no", "51");
	}
}
