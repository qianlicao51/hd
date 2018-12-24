package cn.sxt.day1.hdfs.fof;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.spark_project.guava.collect.Lists;

import cn.sxt.config.HadoopConfig;
import cn.zhuzi.spark.official.SparkUtils;
import scala.Tuple2;

/**
 * 共同好友spark
 * 
 * @author MI
 *
 */
public class SparkFof2 {

	public static void main(String[] args) {

		JavaSparkContext jsContext = SparkUtils.getJavaSparkContext();
		JavaRDD<String> textFile = jsContext.textFile(HadoopConfig.getInputPath("data/sxt/friend"));
		textFile.persist(StorageLevel.MEMORY_AND_DISK_SER());
		JavaRDD<String> flatMap = textFile.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				ArrayList<String> resuList = new ArrayList<String>();
				String[] split = t.split(" ");
				// 此处只是给 一个人的共同好友佩对 比如 tom hello hadoop cat
				// 就输出 hello:hadoop ,hello:cat ,hadoop:cat ,
				// 然后 按照 Wordcount 那样求和 此处出现 一个问题，
				// TODO 这样计算过程中，他俩是别人的好友，可能存储在他俩是直接好友
				for (int i = 1; i < split.length; i++) {
					resuList.add("0=" + FofMapper.friends(split[i], split[0]));// 表示直接好友
					for (int j = i + 1; j < split.length; j++) {
						resuList.add("1=" + FofMapper.friends(split[i], split[j]));// 表示间接好友
					}
				}
				return resuList.iterator();
			}
		});

		// 把 1=hello:hadoop 转为 key=hello:hadoop value=1
		JavaPairRDD<String, String> mapToPair = flatMap
				.mapToPair(t -> new Tuple2<String, String>(t.split("=")[1], t.split("=")[0]));
		// 把上述 按照 key 统计 val 例如 key=hello:hadoop value=集合{1,1,0}包含0 表示他俩是直接好友 ，之后排除
		JavaPairRDD<String, Iterable<String>> groupByKey = mapToPair.groupByKey();
		// 注意 2：此处 过滤参考的是https://www.cnblogs.com/zhoudayang/p/5008227.html “针对2
		// 中程序生成的PairRDD,删选掉长度超过20个字符的”
		// 因为包含 0 表示 他俩只直接好友，需要排除

		// 注意 3： 下面这个过滤失败 是因为Iterable转成List 后判断的，
		// https://blog.csdn.net/u012848709/article/details/85240530
		// = groupByKey.filter(t -> (!(Arrays.asList(t._2())).contains(0)));

		JavaPairRDD<String, Iterable<String>> filter = groupByKey
				.filter(t -> !Lists.newArrayList(t._2()).contains("0"));
		JavaRDD<Tuple2<String, String>> rdd = filter
				.map(new Function<Tuple2<String, Iterable<String>>, Tuple2<String, String>>() {
					@Override
					public Tuple2<String, String> call(Tuple2<String, Iterable<String>> v1) throws Exception {
						int sum = 0;
						for (String s : v1._2) {
							sum += Integer.parseInt(s);
						}
						return new Tuple2<String, String>(v1._1, sum + "");
					}
				});
		// 挑选间接好友操作 把 之前的key 的 前面表示符号去掉
		List<Tuple2<String, String>> collect = rdd.collect();

		for (Tuple2<String, String> tup : collect) {
			System.out.println(tup);
		}

		jsContext.close();

	}
}
