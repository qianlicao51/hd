/**  
 * All rights Reserved, Designed By grq
 * @Title:  MovieAnalysis.java   
 * @Package spark.study.booka.char12   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: grq  
 * @date:   2019年1月2日 下午11:04:32   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.booka.char12;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * @author MI
 *
 */
public class MovieAnalysis {

	/**
	 * 1:RDD实现电影流行度 (1):所有电影中平均得分最高的Top10电影Bysql
	 * 
	 */
	public static void rddForMovieTop10Bysql(SparkSession sparkSession, Dataset<Row> ratDF) {
		System.out.println("--------------------SparkSQL方式 --------------------");
		ratDF.createOrReplaceTempView("t_rat");
		Dataset<Row> sql = sparkSession.sql("select * from ("//
				+ "select avg(rat) rat_avg ,MovieID  from t_rat group by MovieID order by rat_avg desc" //
				+ " ) limit 10");
		System.out.println("所有电影中平均得分最高的Top20电影 SQL方式");
		sql.show();
		System.out.println("所有电影粉丝最多的电影");
		sparkSession.sql("select * from ("//
				+ "select MovieID, count(MovieID) count_moive    from t_rat group by MovieID order by count_moive desc" //
				+ " ) limit 10").show();

	}

	/**
	 * 1:RDD实现电影流行度 (1):所有电影中平均得分最高的Top10电影
	 * 
	 */

	public static void rddForMovieTop10(SparkSession sparkSession, JavaRDD<String> ratRdd) {
		/**
		 * 1 所有电影中平均得分最高的Top10电影
		 */
		// step 1 把数据变为key-value ，eg (MovieID,(Rating,1))
		JavaPairRDD<String, Tuple2<Long, Long>> mapToPair = ratRdd.mapToPair(new PairFunction<String, String, Tuple2<Long, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<Long, Long>> call(String t) throws Exception {
				String[] split = t.split("::");// UserID::MovieID::Rating::Timestamp
				Tuple2<Long, Long> tuple2 = new Tuple2<Long, Long>(Long.valueOf(split[2]), 1L);
				return new Tuple2<String, Tuple2<Long, Long>>(split[1], tuple2);
			}
		});
		mapToPair.cache();
		// step 2 通过reduceByKey 汇总，key是MovieID，但是values是(评分总和，点评人数合计)
		JavaPairRDD<String, Tuple2<Long, Long>> reduceByKey = mapToPair.reduceByKey(new Function2<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Long> call(Tuple2<Long, Long> v1, Tuple2<Long, Long> v2) throws Exception {
				return new Tuple2<Long, Long>(v1._1 + v2._1, v1._2 + v2._2);
			}
		});
		// step 3 sortByKey(false) 倒序排列
		JavaPairRDD<Double, String> result = reduceByKey.mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Long>>, Double, String>() {
			@Override
			public Tuple2<Double, String> call(Tuple2<String, Tuple2<Long, Long>> v1) throws Exception {
				// TODO 都是整数 做除法还是整数
				double avg = v1._2._1 * 0.1 / v1._2._2;
				return new Tuple2<Double, String>(avg, v1._1);
			}
		});
		System.out.println("所有电影中平均得分最高的Top10电影 RDD方式");
		result.sortByKey(false).take(10).forEach(t -> System.out.println(t));

		/**
		 * 2 所有电影粉丝最多的电影
		 */
		System.out.println("所有电影粉丝最多的电影");
		JavaPairRDD<String, Long> reduceByKey2 = mapToPair.mapToPair(t -> new Tuple2<String, Long>(t._1, t._2._2)).reduceByKey((x, y) -> (x + y));
		reduceByKey2.mapToPair(t -> new Tuple2<Long, String>(t._2, t._1)).sortByKey(false).mapToPair(t -> new Tuple2<String, Long>(t._2, t._1)).take(10).forEach(t -> System.out.println(t));
	}
}