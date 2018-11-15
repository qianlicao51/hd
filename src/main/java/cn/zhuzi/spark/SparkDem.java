package cn.zhuzi.spark;

import java.io.File;
import java.io.IOException;

import org.apache.ibatis.io.Resources;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Before;

/**
 * @Title: Demo.java
 * @Package cn.zhuzi.spark
 * @Description: TODO(spark demo)
 * @author 作者 grq
 * @version 创建时间：2018年11月15日 下午4:24:24
 *
 */
public class SparkDem {
	static SparkConf conf;
	static JavaSparkContext sc;

	/**
	 * 创建SparkContext
	 * 的最基本方法，只需要传递2个参数https://www.cnblogs.com/Forever-Road/p/7351245.html
	 * <p/>
	 * 1:集群URL 告诉spark如何连接到集群上，这个实例中使用local，这个特殊的值可以让spark运行在 单机单线程上而不需要连接到集群
	 * <p/>
	 * 2:应用名：当连接到一个集群时，这个值可以帮助你在集群管理器的用户界面中找到你的应用
	 */
	static {
		conf = new SparkConf().setMaster("local").setAppName("sparkDemo");
		sc = new JavaSparkContext(conf);
	}

	/**
	 * 统计包含INFO的行数
	 * 
	 * @throws IOException
	 */
	public static void infoCount() throws IOException {
		File asFile = Resources.getResourceAsFile("info.txt");
		JavaRDD<String> inputRDD = sc.textFile(asFile.getAbsolutePath());
		JavaRDD<String> infoRDD = inputRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String x) throws Exception {
				return x.contains("INFO");
			}
		});
		System.out.println("包含INFO的行数有:" + infoRDD.count());

		// INFO的行数是38行，下面的数字40>38程序不会报错，只是打印了全部包含INFO的行
		for (String line : infoRDD.take(40)) {
			System.out.println(line);
		}
	}

	public static void main(String[] args) throws IOException {
		infoCount();
	}
}
