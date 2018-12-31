/**  
 * All rights Reserved, Designed By www.tydic.com
 * @Title:  APIDemo.java   
 * @Package cn.sxt.spark.core   
 * @Description:    TODO(RDD算子示例)   
 * @author: grq  
 * @date:   2018年12月31日 上午11:01:33   
 * @version V1.0 
 * @Copyright: 2018 grq All rights reserved. 
 */
package cn.sxt.spark.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * @author MI
 *
 */
public class APIDemo {
	static JavaSparkContext sc;
	static String base_path = null;
	/**
	 * Spark运行模式 1.local --Eclipse 开发本地模式 多用于测试
	 * <p>
	 * 2.standalone --Spark 自带的资源调度框架
	 * <p>
	 * 支持分布式搭建，Spark任务可以依赖standalone调度资源
	 * <p>
	 * 3.yarn -- Hadoop 生态圈资源框架 Spark也支持yarn
	 */
	static {
		sc = new JavaSparkContext(SparkSession.builder().appName("JavaSparkPi").master("local").getOrCreate().sparkContext());
	}

	public static void main(String[] args) {
		accumulator();
	}

	/**
	 * 广播变量 Accumulator
	 * <p>
	 * 相当于 集群中统筹大变量
	 * <p>
	 * 1 累加器只能在Driver定义初始化，不能再Executor端定义初始化
	 * <p>
	 * 2 累加器取值 value 只能在Driver读取，不能在Executor端读取
	 * <p>
	 */
	private static void accumulator() {
		Accumulator<Integer> intAccumulator = sc.intAccumulator(0);
		JavaRDD<String> map = rdd.map(new Function<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				intAccumulator.add(1);
				return v1;
			}
		});
		map.collect();
		System.out.println("单词个数是:" + intAccumulator.value());
	}

	static JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello", "spark", "hadoop", "李", "帅", "弓", "瑞", "杰", "青"), 3);

	/**
	 * 重新分区 repartition=coalesce(numPartitions, false)
	 */
	public static void repatition() {
		int size = rdd.partitions().size();
		System.out.println(size);
		JavaRDD<String> coalesce = rdd.coalesce(2, false);// false 不产生shuffle

		// repartition 是有shuffle的算子，可以多RDD重新分区，可以增加或者减少分区
		rdd.repartition(2);
		System.out.println(coalesce.collect());
	}

	/**
	 * group
	 */
	public static void group() {
		JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("grq", 25), new Tuple2<String, Integer>("grq", 22), new Tuple2<String, Integer>("zs", 23),
				new Tuple2<String, Integer>("ls", 24), new Tuple2<String, Integer>("ww", 25)));
		JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(new Tuple2<String, String>("grq", "java"), new Tuple2<String, String>("grq", "spark"),
				new Tuple2<String, String>("zs", "java"), new Tuple2<String, String>("ls", "hive"), new Tuple2<String, String>("ww", "hadoop")));

		JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<String>>> cogroup = rdd1.cogroup(rdd2);

		for (Tuple2<String, Tuple2<Iterable<Integer>, Iterable<String>>> iteam : cogroup.collect()) {
			System.out.println(iteam);
		}
		sc.close();
		// (ls,([24],[hive]))
		// (grq,([25, 22],[java, spark]))
		// (ww,([25],[hadoop]))
		// (zs,([23],[java]))
	}

	public static void mapMethod() {
		JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello", "spark", "hadoop", "李丰", "帅", "弓", "瑞", "杰", "青"), 3);

		JavaRDD<String> map = rdd.map(new Function<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				System.out.println("创建数据库连接……");
				System.out.println("insert数据……         " + v1);
				System.out.println("close数据库连接……");
				return v1;
			}
		});
		map.collect();
	}

	/**
	 * mapPartitions 有几个分区创建几个连接 ，相比 map 性能高一点
	 */
	public static void mapPartitionsMethod() {
		JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello", "spark", "hadoop", "李丰", "帅", "弓", "瑞", "杰", "青"), 3);
		JavaRDD<String> mapPartitions = rdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Iterator<String> t) throws Exception {
				ArrayList<String> list = new ArrayList<String>();
				System.out.println("创建数据库连接……");
				while (t.hasNext()) {
					String next = t.next();
					list.add(next);
					System.out.println("insert数据……         " + next);
				}
				System.out.println("close数据库连接……");
				return list.iterator();
			}
		});
		mapPartitions.collect();
	}

}
