package cn.zhuzi.spark2;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @Title: SparkSQLUtils.java
 * @Package cn.zhuzi.spark2
 * @Description: TODO(Spark 2 版 创建SparkSQL)
 * @author 作者 grq
 * @version 创建时间：2018年11月17日 下午8:00:18
 *
 */
public class SparkSQLUtils {
	private SparkSQLUtils() throws Exception {
		throw new Exception("不允许实例化");
	}

	/**
	 * SparkSession创建方式
	 */
	public static SparkSession buildSparkSession() {
		SparkSession sparkSession = SparkSession.builder().appName("JavaSparkPi").master("local").getOrCreate();
		return sparkSession;
	}

	/**
	 * 官方给的创建JavaSparkContext方式
	 * 
	 * @return
	 */
	public static JavaSparkContext getJavaSparkContext() {
		SparkSession sparkSession = buildSparkSession();
		SparkContext sparkContext = sparkSession.sparkContext();
		return new JavaSparkContext(sparkContext);

	}
}
