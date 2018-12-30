package cn.sxt.my.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import cn.zhuzi.spark.official.SparkUtils;
import scala.Tuple2;
/**
 * Spark 去重
 * @author MI
 *
 */
public class DistinctSpark {
	public static void main(String[] args) {
		JavaSparkContext sparkContext = SparkUtils.getJavaSparkContext();

		JavaRDD<String> parallelize = sparkContext.parallelize(Arrays.asList("hello word", "hello word", "hello word", "hello tom"));
		parallelize.persist(StorageLevel.MEMORY_ONLY());
		System.out.println(parallelize.distinct().collect());

		JavaRDD<String> keys = parallelize.mapToPair(t -> new Tuple2<String, Integer>(t, 1)).reduceByKey((a,b)->a+b).keys();
		System.out.println(keys.collect());

	}
}
