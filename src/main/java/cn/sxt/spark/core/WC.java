/**  
 * All rights Reserved, Designed By www.tydic.com
 * @Title:  WC.java   
 * @Package cn.sxt.spark.core   
 * @Description:    TODO(提交集群测试demo)   
 * @author: grq  
 * @date:   2018年12月31日 上午12:11:45   
 * @version V1.0 
 * @Copyright: 2018 grq All rights reserved. 
 */
package cn.sxt.spark.core;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * @author MI
 *
 */
public class WC {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(SparkSession.builder().appName("JavaSpark_wordCount").getOrCreate().sparkContext());
		JavaRDD<String> parallelize = sc.parallelize(Arrays.asList("hello grq", "hello lifeng", "hello lifeng", "hello lifeng"));

		JavaPairRDD<String, Integer> mapToPair = parallelize.mapToPair(t -> new Tuple2<String, Integer>(t, 1)).reduceByKey((a, b) -> (a + b)).mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1))
				.sortByKey(Boolean.FALSE).mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1));

		mapToPair.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println("key:" + t._1 + "  value:" + t._2);
			}
		});
		sc.close();
	}

}
