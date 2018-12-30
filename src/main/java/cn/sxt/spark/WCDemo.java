/**  
 * All rights Reserved, Designed By www.tydic.com
 * @Title:  WCDemo.java   
 * @Package cn.sxt.spark   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: grq  
 * @date:   2018年12月30日 下午8:08:56   
 * @version V1.0 
 * @Copyright: 2018 grq All rights reserved. 
 */
package cn.sxt.spark;

import java.io.IOException;
import java.util.Arrays;

import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * @author MI
 *
 */
public class WCDemo {
	static JavaSparkContext sc;
	/**
	 * Spark运行模式 1.local --Eclipse 开发本地模式 多用于测试
	 * <p>
	 * 2.standalone --Spark 自带的资源调度框架
	 * <p>
	 * 支持分布式搭建，Spark任务可以依赖standalone调度资源
	 * <p>
	 * 3.yarn -- Hadoop 生态圈资源框架 Spark也支持yarn
	 */
	static {

		sc = new JavaSparkContext(SparkSession.builder().appName("JavaSparkPi").master("local").getOrCreate().sparkContext());

	}

	public static void main(String[] args) throws IOException {
		/**
		 * 使用mybatis 获取资源文件路径
		 */
		String base_path = Resources.getResourceAsFile("data/sxt/hello2.txt").getAbsolutePath();
		JavaPairRDD<String, Integer> mapToPair = sc.textFile(base_path).flatMap(t -> Arrays.asList(t.split(" ")).iterator()).mapToPair(t -> new Tuple2<String, Integer>(t, 1));
		JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey((a, b) -> (a + b));
		// 倒序排序
		JavaPairRDD<String, Integer> mapToPair2 = reduceByKey.mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1)).sortByKey(Boolean.FALSE).mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1));
		for (Tuple2<String, Integer> tuple : mapToPair2.collect()) {
			System.out.println(tuple);
		}

	}
}
