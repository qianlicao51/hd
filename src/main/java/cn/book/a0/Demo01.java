package cn.book.a0;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import cn.zhuzi.spark.official.SparkUtils;
import scala.Tuple2;

public class Demo01 {
	/**
	 * Scala插件地址http://scala-ide.org/download/prev-stable.html
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		JavaSparkContext scContext = SparkUtils.getJavaSparkContext();

		JavaRDD<String> filter = scContext.textFile("hdfs://had2/file/core-site.xml", 2).filter(t -> t.contains("name"));
		int numPartitions = filter.getNumPartitions();

		System.out.println(numPartitions);

		// 根据依赖关系找到源头RDD

	}

	public void testcreateRDD() throws Exception {
		JavaSparkContext sc = SparkUtils.getJavaSparkContext();
		JavaPairRDD<Integer, Integer> pairs = sc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(1, 2), new Tuple2<Integer, Integer>(1, 2)));

		Function f = new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1;
			}
		};
		pairs.flatMapValues(f);
	}
}
