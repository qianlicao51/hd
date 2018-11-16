package cn.zhuzi.spark.chart06;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import cn.zhuzi.spark.SparkUtils;

/**
 * @Title: Demo.java
 * @Package cn.zhuzi.spark.chart06
 * @Description: TODO(累加器)
 * @author 作者 grq
 * @version 创建时间：2018年11月16日 下午10:20:24
 *
 */
public class Demo {
	/**
	 * 累加器，计算空白行
	 */
	public static void callamdba() {
		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> file = sc.textFile("c://info.txt");
		Accumulator<Integer> blankLines = sc.accumulator(0);// 创建并初始化为0
		JavaRDD<String> call = file.flatMap(line -> {
			if (line.length() == 0) {
				// 累加器加1
				blankLines.add(1);
			}
			return Arrays.asList(line.split(" ")).iterator();
		});

		System.out.println(call.count());
		System.out.println("空白行有：" + blankLines);
		sc.close();
	}

	/**
	 * 累加器，计算空白行
	 */
	public static void cal() {
		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> file = sc.textFile("c://info.txt");
		Accumulator<Integer> blankLines = sc.accumulator(0);// 创建并初始化为0

		JavaRDD<String> call = file.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception {
				if (t.length() == 0) {
					blankLines.add(1);
				}
				return Arrays.asList(t.split(" ")).iterator();
			}
		});
		System.out.println(call.count());
		System.out.println("空白行有：" + blankLines);
		sc.close();
	}

	public static void main(String[] args) {
		cal();
	}
}
