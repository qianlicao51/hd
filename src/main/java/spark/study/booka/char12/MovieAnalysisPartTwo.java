/**  
 * All rights Reserved, Designed By grq
 * @Title:  MovieAnalysisPartTwo.java   
 * @Package spark.study.booka.char12   
 * @Description:    TODO(电影分析)   
 * @author: grq  
 * @date:   2019年1月4日 下午9:32:45   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.booka.char12;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

/**
 * @author MI
 *
 */
public class MovieAnalysisPartTwo {

	/**
	 * 年龄18岁最喜爱的电影 博客地址https://blog.csdn.net/u012848709/article/details/85808333
	 * 
	 * @param sparkSession
	 * @param userRdd
	 * @param ratRdd
	 * @param moviesRdd
	 */
	public static void age18PopularByRDD(SparkSession sparkSession, JavaRDD<String> userRdd, JavaRDD<String> ratRdd, JavaRDD<String> moviesRdd) {
		// 用户中挑选年龄18 岁的 key-value :(userid:age)
		JavaPairRDD<String, String> userfilter = userRdd.mapToPair(t -> new Tuple2<String, String>(t.split("::")[0], t.split("::")[2])).filter(t -> t._2.equals("18"));
		List<String> userList = userfilter.map(t -> t._1).collect();// 这个集合不能add

		// Java创建ClassTag的方法https://blog.csdn.net/hhtop112408/article/details/78338716
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
		final Broadcast<List<String>> broadcast = javaSparkContext.broadcast(userList);

		// step 1 电影 (电影id,电影名字)
		JavaPairRDD<String, String> movie_id_name = moviesRdd.mapToPair(t -> new Tuple2<String, String>(t.split("::")[0], t.split("::")[1]));
		movie_id_name.cache();
		Map<String, String> movie_map = movie_id_name.collectAsMap();

		// step 2 评分(用户id，电影id)
		JavaPairRDD<String, String> movieFilter = ratRdd.mapToPair(t -> new Tuple2<String, String>(t.split("::")[0], t.split("::")[0])).filter(new Function<Tuple2<String, String>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				return broadcast.value().contains(v1._1);
			}
		});
		/*
		 * movieFilter 变为 key-value (电影ID,1),然后在进行聚合操作 变为(电影id，观看总次数),然后交换 key-value
		 * 变为(总次数，电影ID) 并降序排序。再次交换key-value
		 */
		JavaPairRDD<String, Integer> movie_count_mid = movieFilter.mapToPair(t -> new Tuple2<String, Integer>(t._2, 1)).reduceByKey((a, b) -> (a + b))
				.mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1)).sortByKey(false).mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1));

		// 打印电影名字 和喜爱人数
		movie_count_mid.take(10).forEach(t -> System.out.println("喜爱人数" + t._2 + "  喜爱的电影名值" + movie_map.get(t._1) + "- "));

	}

}
