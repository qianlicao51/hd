package cn.zhuzi.sparksp02;

import org.apache.spark.api.java.JavaSparkContext;

import cn.zhuzi.spark.official.SparkUtils;

/**
 * @Title: SparkDemo.java
 * @Package cn.zhuzi.sparksp02
 * @Description: TODO(第二个视频学习内容)
 * @author 作者 grq
 * @version 创建时间：2018年11月22日 上午11:16:54
 *
 */
public class SparkDemo {
	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
		System.out.println(sc);
		sc.close();
	}
}
