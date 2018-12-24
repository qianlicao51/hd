package cn.sxt.day1.hdfs.fof;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;

import cn.sxt.config.HadoopConfig;
import cn.zhuzi.spark.official.SparkUtils;
import scala.Tuple2;

public class SparkFof {
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
					for (int j = i + 1; j < split.length; j++) {
						resuList.add(FofMapper.friends(split[i], split[j]));
					}
				}
				return resuList.iterator();
			}
		});
		// 直接好友
		JavaRDD<String> flatMapFriend = textFile.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				ArrayList<String> resuList = new ArrayList<String>();
				String[] split = t.split(" ");
				for (int i = 1; i < split.length; i++) {
					resuList.add(FofMapper.friends(split[i], split[0]));
				}
				return resuList.iterator();
			}
		});
		ArrayList<String>list =new ArrayList<String>(flatMap.collect()) ;
		ArrayList<String> collect2 = new ArrayList<String>(flatMapFriend.collect());
		list.removeAll(collect2);
		// flatMap中药排除他俩是直接好友
		JavaRDD<String> parallelize = jsContext.parallelize(list);
		JavaPairRDD<String, Integer> mapToPair = parallelize.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
		// 分组聚合
		JavaPairRDD<String, Integer> res = mapToPair.reduceByKey((a, b) -> a + b);
		List<Tuple2<String, Integer>> collect = res.collect();
		for (Tuple2<String, Integer> tuple2 : collect) {
			System.out.println(tuple2);
		}
		jsContext.close();

	}
}
