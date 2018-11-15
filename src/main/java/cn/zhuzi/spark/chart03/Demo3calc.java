package cn.zhuzi.spark.chart03;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import cn.zhuzi.spark.SparkUtils;

/**
 * @Title: Demo3calc.java
 * @Package cn.zhuzi.spark
 * @Description: TODO(书第三章 代码实例)
 * @author 作者 grq
 * @version 创建时间：2018年11月15日 下午9:03:56
 *
 */
public class Demo3calc {

	public static void main(String[] args) {
		calc();
	}

	/**
	 * 计算RDD中各值得平方
	 */
	private static void calc() {

		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> result = rdd.map(x -> x * x);

		System.out.println(StringUtils.join(result.collect(), "  "));
	}
}
