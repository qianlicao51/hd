package cn.zhuzi.spark.official;

import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

/**
 * @Title: SparkUtils.java
 * @Package cn.zhuzi.spark.official
 * @Description: TODO(官方创建 spark)
 * @author 作者 grq
 * @version 创建时间：2018年11月19日 下午11:52:23
 *
 */
public class SparkUtils {
	static SparkSession sparkSession;
	static JavaSparkContext sc;
	static {
		if (sparkSession == null) {
			sparkSession = buildSparkSession();
			sc = new JavaSparkContext(sparkSession.sparkContext());
		}
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

	/**
	 * 读取文件
	 * 
	 * @param path
	 */
	public static void readFile(String path) {
		Dataset<String> textFile = sparkSession.read().textFile(path);
		JavaRDD<String> lines = textFile.toJavaRDD();
		lines.persist(StorageLevel.OFF_HEAP());
		List<String> collect = lines.collect();
		for (String strLog : collect) {
			System.out.println(strLog);
		}

	}

}
