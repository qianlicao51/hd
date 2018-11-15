package cn.zhuzi.spark;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;

import scala.Tuple2;

/**
 * @Title: SparkDemoWordCountLamdba.java
 * @Package cn.zhuzi.spark
 * @Description: TODO(spark wordcount lamdba版)
 * @author 作者 grq
 * @version 创建时间：2018年11月15日 下午8:03:12
 *
 */
public class SparkDemoWordCountLamdba {
	public static void main(String[] args) throws IOException {
		String base_path = "E:/had/spark/";
		String inputPath = base_path + "12.txt";
		String outputpath = base_path + "out/a_wc";
		File file = FileUtils.getFile(outputpath + new DateTime().toString("yyyyMMdd_HHmm_ss"));
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}
		wordCount2(inputPath, file.getAbsolutePath());
	}

	/**
	 * https://blog.csdn.net/babysoe/article/details/80210195
	 * 
	 * @param inputPath
	 *            输入路径
	 * @param outputpath
	 *            输出路径
	 */
	public static void wordCount(String inputPath, String outputpath) {
		// 创建spark context
		JavaSparkContext sc = SparkUtils.getContext();

		// 读取数据
		JavaRDD<String> jrdd = sc.textFile(inputPath);
		// 切割压平
		JavaRDD<String> jrdd2 = jrdd.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
		// 和1组合
		JavaPairRDD<String, Integer> jprdd = jrdd2.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
		// 分组聚合
		JavaPairRDD<String, Integer> res = jprdd.reduceByKey((a, b) -> a + b);
		// 保存
		res.saveAsTextFile(outputpath);
		// 释放资源
		sc.close();
	}

	/**
	 * https://databricks.com/blog/2014/04/14/spark-with-java-8.html
	 * 
	 * @param inputPath
	 * @param outputpath
	 */
	public static void wordCount2(String inputPath, String outputpath) {
		// 创建spark context
		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> lines = sc.textFile(inputPath);
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((a, b) -> a + b);
		counts.saveAsTextFile(outputpath);
		System.out.println("========================================");
		System.out.println(counts.collect());// [(grq,3), (12,1), (帅,1)]
		for (Tuple2<String, Integer> tup : counts.collect()) {
			System.out.println(tup._1 + "<--->" + tup._2);
			// grq<--->3
		}
		System.out.println("========================================");
		sc.close();
	}
}
