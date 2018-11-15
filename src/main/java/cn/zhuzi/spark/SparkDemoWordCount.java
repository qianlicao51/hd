package cn.zhuzi.spark;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @Title: SparkDemoWordCount.java
 * @Package cn.zhuzi.spark
 * @Description: TODO(spark 统计单词)
 * @author 作者 grq
 * @version 创建时间：2018年11月15日 下午2:54:44
 *
 */
public class SparkDemoWordCount {

	public static void main(String[] args) throws IOException {
		String base_path = "E:/had/spark/";
		String inputPath = base_path + "12.txt";
		String outputpath = base_path + "out/a_wc";
		File file = FileUtils.getFile(outputpath);
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}
		wordCount(inputPath, outputpath);
	}

	@SuppressWarnings("serial")
	public static <U> void wordCount(String inputPath, String outputpath) {
		// 创建spark context
		JavaSparkContext sc = SparkUtils.getContext();

		// 读取输入的数据
		JavaRDD<String> input = sc.textFile(inputPath);

		// 切分为单词
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String t) throws Exception {
				Iterator<String> iterator = Arrays.asList(t.split(" ")).iterator();
				return iterator;
			}
		});
		// 转换为键值对并基数
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) throws Exception {
				return x + y;
			}
		});
		// 输出到文件。文件必须不存在
		counts.saveAsTextFile(outputpath);
		sc.close();
	}
}
