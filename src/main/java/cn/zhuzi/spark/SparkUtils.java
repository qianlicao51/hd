package cn.zhuzi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * @Title: SparkUtils.java
 * @Package cn.zhuzi.spark
 * @Description: TODO(SparkUtils )
 * @author 作者 grq
 * @version 创建时间：2018年11月15日 下午2:49:33
 *
 */
public class SparkUtils {
	static SparkConf conf;
	static JavaSparkContext sc;
	/**
	 * 初始化sparkContext
	 * 
	 */
	static {
		// TODO 创建SparkContext
		// 的最基本方法，只需要传递2个参数https://www.cnblogs.com/Forever-Road/p/7351245.html
		/**
		 * 1:集群URL 告诉spark如何连接到集群上，这个实例中使用local，这个特殊的值可以让spark运行在 单机单线程上而不需要连接到集群
		 * <p/>
		 * 2:应用名：当连接到一个集群时，这个值可以帮助你在集群管理器的用户界面中找到你的应用
		 */
		buildContext();
	}

	private static void buildContext() {
		conf = new SparkConf().setMaster("local").setAppName("sparkDemo").set("spark.driver.allowMultipleContexts", "true");
		sc = new JavaSparkContext(conf);
	}

	/**
	 * 关闭spark
	 */
	public static void closeContext() {
		sc.close();
		// 或者
		System.exit(0);
	}

	private SparkUtils() throws Exception {
		throw new Exception("不允许实例化工具类");
	}

	public static JavaSparkContext getContext() {
		if (sc == null) {
			buildContext();
		}
		return sc;
	}

	/**
	 * Spark2.0之前版本SQLContext创建方式
	 * 
	 * @return SQLContext
	 */
	public static SQLContext getSQLContextOld() {
		SQLContext sqlContext = new SQLContext(sc);
		return sqlContext;
	}

	/**
	 * SparkSession创建方式
	 * 
	 * @return
	 */
	public static SparkSession buildSparkSession() {
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		return sparkSession;
	}

	/**
	 * SQLContext创建
	 * 
	 * @return SQLContext
	 */
	public static SQLContext getSQLContext() {
		SparkSession sparkSession = buildSparkSession();
		SQLContext context = new SQLContext(sparkSession);
		return context;
	}

	public static void main(String[] args) {
		buildSparkSession();
	}

	/**
	 * SparkContext
	 * 
	 * @return
	 */
	public static SparkContext getSparkContext() {
		SparkContext sparkContext = new SparkContext(conf);
		return sparkContext;
	}
}
