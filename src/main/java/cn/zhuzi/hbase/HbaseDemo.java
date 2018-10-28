package cn.zhuzi.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

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
	static {
		// 使用HBaseConfiguration 单例方法实例化
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop");// 单机
		// conf.set("hbase.zookeeper.quorum", "master,work1,work2");//
		// zookeeper地址
		conf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
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
		@SuppressWarnings("resource")
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		return admin.tableExists(tableName);

	}

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		boolean tableExists = isTableExists("stu");
		System.out.println("HbaseDemo.main()" + tableExists);
	}
}
