package cn.zhuzi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Title: SparkDemo.java
 * @Package cn.zhuzi.spark
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年11月15日 下午2:20:09
 *
 */
public class SparkDemo {
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
		 * 1:集群URL 告诉spark如何连接到集群上，这个实例中使用local，这个特殊的值可以让spark运行在
		 * 单机单线程上而不需要连接到集群 2:应用名：当连接到一个集群时，这个值可以帮助你在集群管理器的用户界面中找到你的应用
		 */
		conf = new SparkConf().setMaster("local").setAppName("sparkDemo");
		sc = new JavaSparkContext(conf);
	}

	public static void main(String[] args) {
		System.out.println(sc);
	}
}
