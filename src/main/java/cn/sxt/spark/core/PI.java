/**  
 * All rights Reserved, Designed By www.tydic.com
 * @Title:  PI.java   
 * @Package cn.sxt.spark.core   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: grq  
 * @date:   2018年12月31日 下午1:35:23   
 * @version V1.0 
 * @Copyright: 2018 grq All rights reserved. 
 */
package cn.sxt.spark.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author MI
 *
 */
public class PI {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("JavaSparkPi").getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		int slices = 100;
		int n = 100000 * slices;
		List<Integer> l = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			l.add(i);
		}

		JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

		int count = dataSet.map(integer -> {
			double x = Math.random() * 2 - 1;
			double y = Math.random() * 2 - 1;
			return (x * x + y * y <= 1) ? 1 : 0;
		}).reduce((integer, integer2) -> integer + integer2);

		System.out.println("Pi is roughly " + 4.0 * count / n);

		spark.stop();

	}
}
