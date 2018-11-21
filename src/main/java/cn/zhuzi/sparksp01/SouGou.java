package cn.zhuzi.sparksp01;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;

import scala.Tuple2;

/**
 * @Title: SouGou.java
 * @Package cn.zhuzi.sparksp01
 * @Description: TODO(搜狗查询日志排行)
 * @author 作者 grq
 * @version 创建时间：2018年11月21日 上午11:03:30
 *
 */
public class SouGou {
	static SparkSession sparkSession;
	static JavaSparkContext sc;
	static {
		if (sparkSession == null) {
			sparkSession = buildSparkSession();
			sc = new JavaSparkContext(sparkSession.sparkContext());
		}
	}

	public static void main(String[] args) throws IOException {
		// sortBysearch();
		// sort();
		sortLogByKey();
	}

	/**
	 * 测试排序
	 */
	public static void sort() {
		List<Integer> takeOrdered = sc.parallelize(Arrays.asList(2, 3, 4, 5, 6)).takeOrdered(2);
		System.out.println(takeOrdered);

	}

	/**
	 * 热搜词排序
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private static void sortLogByKey() throws IOException {
		String filePath = Resources.getResourceAsFile("data/txt/20.TXT").getAbsolutePath();

		JavaRDD<String> fileStrRdd = sc.textFile(filePath);
		JavaRDD<String> filter = fileStrRdd.filter(t -> t.split("\t").length == 6);
		JavaPairRDD<String, Integer> mapToPair = filter.mapToPair(t -> (new Tuple2<String, Integer>((t.split("\t")[2]), 1)));
		JavaPairRDD<String, Integer> resuleRDD = mapToPair.reduceByKey((a, b) -> a + b).mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1)).sortByKey(false).mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1));
		resuleRDD.persist(StorageLevel.DISK_ONLY());
		List<Tuple2<String, Integer>> collect = resuleRDD.collect();
		for (Tuple2<String, Integer> tup : collect) {
			System.out.println(tup._1 + "<>" + tup._2);

		}
		// System.out.println(resuleRDD.top(10));//报错
		// 获取前10个
		System.out.println(resuleRDD.collect().subList(0, 10));
		File file = FileUtils.getFile("E:/had/spark/out/a_wc" + new DateTime().toString("yyyyMMdd_HHmm_ss"));
		resuleRDD.saveAsTextFile(file.getAbsolutePath());
	}

	/**
	 * 模拟wordcount 按照 词数量倒序排列
	 */
	public static void sortBysearch() {

		JavaRDD<String> lines = sc.parallelize(Arrays.asList("ahello", "bwod", "grq", "grq", "grq"));
		JavaPairRDD<String, Integer> reduceByKey = lines.mapToPair(t -> new Tuple2<String, Integer>(t, 1)).reduceByKey((a, b) -> a + b).sortByKey();
		System.out.println(reduceByKey.collect());
		// [(ahello,1), (bwod,1), (grq,3)]

		// TODO 此处排序，之后就不要排序了
		JavaPairRDD<Integer, String> secondStepRdd = reduceByKey.mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1)).sortByKey(false);
		System.out.println(secondStepRdd.collect());
		// [(3,grq), (1,ahello), (1,bwod)]

		JavaPairRDD<String, Integer> resuleRdd = secondStepRdd.mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1));
		System.out.println(resuleRdd.collect());
		// [(grq,3), (ahello,1), (bwod,1)]
	}

	/**
	 * 官方例子构建session的方法
	 */
	public static SparkSession buildSparkSession() {
		SparkSession sparkSession = SparkSession.builder().appName("JavaSparkPi")
		// .master("spark://hadoop:7077")远程地址
				.master("local").getOrCreate();
		return sparkSession;
	}
}
