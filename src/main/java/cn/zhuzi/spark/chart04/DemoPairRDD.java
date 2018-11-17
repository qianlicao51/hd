package cn.zhuzi.spark.chart04;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import cn.zhuzi.spark.SparkUtils;

/**
 * @Title: DemoPairRDD.java
 * @Package cn.zhuzi.spark.chart04
 * @Description: TODO(键值对 pair RDD)
 * @author 作者 grq
 * @version 创建时间：2018年11月16日 上午10:19:34
 *
 */
public class DemoPairRDD {
	/**
	 * java中使用一个单词作为键创建出一个Pair RDD
	 */
	public static void getPairRDD() {
		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				return new Tuple2<String, String>(t.split(" ")[0], t);
			}

		};
		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello word", "spark good"));
		JavaPairRDD<String, String> javaPairRDD = lines.mapToPair(keyData);
		System.out.println(javaPairRDD.collect());
		// [(hello,hello word), (spark,spark good)]

	}

	/**
	 * java中使用一个单词作为键创建出一个Pair RDD
	 * https://blog.csdn.net/leen0304/article/details/78743715
	 * 
	 * @return
	 */
	public static JavaPairRDD<String, String> getPairRDDLamdba() {
		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello word", "spark good"));
		// TODO 下面的语句 在new Tuple2 里面写 split 不自动提示，难道 lamdba有部分不提示吗？
		JavaPairRDD<String, String> counts = lines.mapToPair(t -> (new Tuple2<String, String>(t.split(" ")[0], t)));
		System.out.println(counts.collect());
		// [(hello,hello word), (spark,spark good)]
		return counts;
	}

	/**
	 * 下面内容是 pair RDD 的转换内容了解
	 * <p>
	 * start
	 */

	/**
	 * 对第二个元素进行筛选
	 */
	public static void selectPairRDD() {
		JavaPairRDD<String, String> pairRDD = getPairRDDLamdba();
		JavaPairRDD<String, String> filter = pairRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				return v1._2().length() < 20;
			}
		});
		System.out.println(filter.collect());
		// [(hello,hello word), (spark,spark good)]
	}

	/**
	 * 对第二个元素进行筛选
	 */
	public static void selectPairRDDlamdba() {
		JavaPairRDD<String, String> pairRDD = getPairRDDLamdba();
		// TODO 下面的语句 eclipse没提示？
		JavaPairRDD<String, String> filter = pairRDD.filter(t -> t._2().length() < 20);

		// [(hello,hello word), (spark,spark good)]
		System.out.println(filter.collect());
	}

	/**
	 * 单词统计
	 * 
	 * @throws IOException
	 */
	public static void wc() throws IOException {
		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> inputRDD = sc.parallelize(Arrays.asList("hello word", "hello spark"));
		JavaRDD<String> words = inputRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairRDD<String, Integer> groupByKey = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((x, y) -> x + y);
		System.out.println(groupByKey.collect());
		// [(spark,1), (word,1), (hello,2)]

	}

	/**
	 * 单词统计
	 * 
	 * @throws IOException
	 */
	public static void wc2() throws IOException {
		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> inputRDD = sc.parallelize(Arrays.asList("hello word", "hello spark"));
		Map<String, Long> words = inputRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).countByValue();
		System.out.println(words);
		// {spark=1, word=1, hello=2}
	}

	/**
	 * 排序
	 * <p>
	 * Java中以字符串顺序自定义排序
	 */

	public static void orderBy() {
		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> inputRDD = sc.parallelize(Arrays.asList("hello word", "hello spark"));
		JavaRDD<String> words = inputRDD.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
		JavaPairRDD<String, Integer> result = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((x, y) -> x + y);
		System.out.println(result.collect());
		// [(spark,1), (word,1), (hello,2)]

		class Compa implements Serializable, Comparator<String> {
			private static final long serialVersionUID = 1L;

			@Override
			public int compare(String a, String b) {
				return a.compareTo(b);
			}
		}
		Compa compa = new Compa();
		JavaPairRDD<String, Integer> sortByKey = result.sortByKey(compa);
		System.err.println(sortByKey.collect());
		// [(hello,2), (spark,1), (word,1)]
		sc.close();
	}

	/**
	 * 下面内容是 pair RDD 的转换内容了解
	 * <p>
	 * end
	 * 
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// getPairRDD();
		// getPairRDDLamdba();
		// selectPairRDD();
		// selectPairRDDlamdba();
		// orderBy();
		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> rdd = sc.textFile("c:/*.txt");
		System.out.println(rdd.filter(t -> t.contains("INFO")).count());

	}
}