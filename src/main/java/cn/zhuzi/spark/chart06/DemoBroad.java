package cn.zhuzi.spark.chart06;

import org.apache.spark.api.java.JavaSparkContext;

import cn.zhuzi.spark.SparkUtils;

/**
 * @Title: DemoBroad.java
 * @Package cn.zhuzi.spark.chart06
 * @Description: TODO(广播变量)
 * @author 作者 grq
 * @version 创建时间：2018年11月16日 下午11:02:15
 *
 */
public class DemoBroad {
	/**
	 * 广播变量查询国家
	 */
	public static void brod() {

		JavaSparkContext sc = SparkUtils.getContext();

		sc.close();
	}

	public static void main(String[] args) {

	}
}
