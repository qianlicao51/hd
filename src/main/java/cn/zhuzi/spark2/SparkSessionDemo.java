package cn.zhuzi.spark2;

import java.io.IOException;

import org.apache.ibatis.io.Resources;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSON;

import scala.Function1;
import scala.Tuple2;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;

/**
 * @Title: SparkSessionDemo.java
 * @Package cn.zhuzi.spark2
 * @Description: TODO(SparkSession 创建)
 * @author 作者 grq
 * @version 创建时间：2018年11月17日 下午10:45:23
 *
 */
public class SparkSessionDemo {

	/**
	 * 2.0版本创建sparkSession
	 */
	public static SparkSession buildSparkSession() {
		SparkSession sparkSession = SparkSession.builder().appName("MyLocal").master("local").config("key", "value").getOrCreate();
		return sparkSession;
	}

	/**
	 * 2.0版本创建支持hive的sparkSession
	 */
	public static SparkSession buildSparkSessionEnableHive() {
		SparkSession sparkSession = SparkSession.builder().appName("MyLocal").master("local").config("key", "value").enableHiveSupport().getOrCreate();
		return sparkSession;
	}

	/**
	 * 读取 json
	 */
	public static void readJson() throws IOException {
		Resources.getResourceAsFile("json/person.json").getAbsolutePath();

	}

	public static void main(String[] args) {

	}

	private static void conf() {
		SparkSession sparkSession = buildSparkSessionEnableHive();
		RuntimeConfig runtimeConfig = sparkSession.conf();
		Map<String, String> confAll = runtimeConfig.getAll();
		System.out.println(confAll);
		/**
		 * <br>
		 * spark.driver.host -> 169.254.86.190</br> <br>
		 * spark.driver.port -> 59254</br> <br>
		 * hive.metastore.warehouse.dir ->
		 * file:/E:/lun/work/hd/spark-warehouse/</br> <br>
		 * spark.app.name -> MyLocal</br> <br>
		 * key -> value</br> <br>
		 * spark.executor.id -> driver</br> <br>
		 * spark.master -> local</br> <br>
		 * spark.app.id -> local-1542467177838</br>
		 */
	}
}
