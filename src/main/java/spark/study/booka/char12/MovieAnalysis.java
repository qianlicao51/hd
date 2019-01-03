/**  
 * All rights Reserved, Designed By grq
 * @Title:  MovieAnalysis.java   
 * @Package spark.study.booka.char12   
 * @Description:    TODO(电影推荐)   
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
import org.apache.spark.sql.functions;
import org.joda.time.DateTime;

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

	/**
	 * 2:最受男性欢迎的电影 和最受女性欢迎的电影(RDD方式)
	 * 
	 * @param userDF
	 * @param ratDF
	 */
	public static void popularByRDD(SparkSession sparkSession, JavaRDD<String> userRdd, JavaRDD<String> ratRdd) {
		System.out.println("男性喜爱的10个电影 ByRDD");
		System.out.println(new DateTime().toString("yyyy-MMM-dd HH:mm:ss:SSS"));
		// UserID::Gender
		JavaPairRDD<String, String> user_gender = userRdd.mapToPair(t -> new Tuple2<String, String>(t.split("::")[0], t.split("::")[1]));
		user_gender.cache();

		// 评分变为 userid:(电影id,评分)
		JavaPairRDD<String, Tuple2<String, Long>> user_movie_rat = ratRdd.mapToPair(new PairFunction<String, String, Tuple2<String, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<String, Long>> call(String t) throws Exception {
				String[] split = t.split("::");// UserID::MovieID::Rating::Timestamp
				Tuple2<String, Long> tuple2 = new Tuple2<String, Long>(split[1], Long.valueOf(split[2]));// 电影id+评分
				return new Tuple2<String, Tuple2<String, Long>>(split[0], tuple2);

			}
		});
		user_movie_rat.cache();
		JavaPairRDD<String, Tuple2<Tuple2<String, Long>, String>> user_pairRdd = user_movie_rat.join(user_gender);
		// user_pairRdd.take(10).forEach(t -> System.out.println(t));
		// (2828,((3793,3),M))
		// (2828,((2997,5),M))
		// (2828,((3005,4),M))
		// 从里面过滤男性
		JavaPairRDD<String, Tuple2<Tuple2<String, Long>, String>> filter = user_pairRdd.filter(new Function<Tuple2<String, Tuple2<Tuple2<String, Long>, String>>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Tuple2<Tuple2<String, Long>, String>> v1) throws Exception {
				// 取出男性M 此处 v1._2._1 是 (3793,3) | v1._2._2 是 M
				return v1._2._2.equals("M");
			}
		});

		// 将上述过滤之后的结果 (userid,(电影id,评分),性别) 从新构造成 (MovieID,(Rating,1))

		JavaPairRDD<String, Tuple2<Long, Long>> mapToPair = filter.mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Long>, String>>, String, Tuple2<Long, Long>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Tuple2<Long, Long>> call(Tuple2<String, Tuple2<Tuple2<String, Long>, String>> v) throws Exception {
				Tuple2<Long, Long> tuple2 = new Tuple2<Long, Long>(v._2._1._2, 1L);
				// System.out.println(new Tuple2<String, Tuple2<Long, Long>>(v._2._1._1,
				// tuple2));// (1270,(4,1))
				// 此处的 v._2._1._1 确实感觉判断挺烦，可以打印看看
				return new Tuple2<String, Tuple2<Long, Long>>(v._2._1._1, tuple2);
			}
		});
		/**
		 * 1 所有电影中平均得分最高的Top10电影
		 */
		// step 1 把数据变为key-value ，eg (MovieID,(Rating,1))
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
		result.sortByKey(false).take(10).forEach(t -> System.out.println(t));
		System.out.println(new DateTime().toString("yyyy-MMM-dd HH:mm:ss:SSS"));
	}

	/**
	 * 2:最受男性欢迎的电影 和最受女性欢迎的电影(RDD方式)使用lambda简化
	 * 
	 * @param userDF
	 * @param ratDF
	 */
	public static void popularByRDDSimpleness(SparkSession sparkSession, JavaRDD<String> userRdd, JavaRDD<String> ratRdd) {
		System.out.println("男性喜爱的10个电影 ByRDD");
		System.out.println(new DateTime().toString("yyyy-MMM-dd HH:mm:ss:SSS"));
		// UserID::Gender
		JavaPairRDD<String, String> user_gender = userRdd.mapToPair(t -> new Tuple2<String, String>(t.split("::")[0], t.split("::")[1]));
		user_gender.cache();
		// 评分变为 userid:(电影id,评分)
		JavaPairRDD<String, Tuple2<String, Long>> user_movie_rat = ratRdd
				.mapToPair(t -> new Tuple2<String, Tuple2<String, Long>>(t.split("::")[0], new Tuple2<String, Long>(t.split("::")[1], Long.valueOf(t.split("::")[2]))));

		user_movie_rat.cache();
		JavaPairRDD<String, Tuple2<Tuple2<String, Long>, String>> user_pairRdd = user_movie_rat.join(user_gender);
		// user_pairRdd.take(10).forEach(t -> System.out.println(t));
		// (2828,((3793,3),M))
		// (2828,((2997,5),M))
		// 从里面过滤男性
		JavaPairRDD<String, Tuple2<Tuple2<String, Long>, String>> filter = user_pairRdd.filter(t -> t._2._2.equals("M"));

		// 将上述过滤之后的结果 (userid,(电影id,评分),性别) 从新构造成 (MovieID,(Rating,1))
		JavaPairRDD<String, Tuple2<Long, Long>> mapToPair = filter.mapToPair(t -> new Tuple2<String, Tuple2<Long, Long>>(t._2._1._1, new Tuple2<Long, Long>(t._2._1._2, 1L)));

		/**
		 * 1 所有电影中平均得分最高的Top10电影
		 */
		// step 1 把数据变为key-value ，eg (MovieID,(Rating,1))
		mapToPair.cache();
		// step 2 通过reduceByKey 汇总，key是MovieID，但是values是(评分总和，点评人数合计)
		JavaPairRDD<String, Tuple2<Long, Long>> reduceByKey = mapToPair.reduceByKey((a, b) -> new Tuple2<Long, Long>(a._1 + b._1, a._2 + b._2));
		// step 3 sortByKey(false) 倒序排列
		JavaPairRDD<Double, String> result = reduceByKey.mapToPair(v1 -> new Tuple2<Double, String>((v1._2._1 * 0.1 / v1._2._2), v1._1));
		result.sortByKey(false).take(10).forEach(t -> System.out.println(t));
		System.out.println(new DateTime().toString("yyyy-MMM-dd HH:mm:ss:SSS"));
	}

	/**
	 * 2:最受男性欢迎的电影 和最受女性欢迎的电影(Sql方式)
	 * 
	 * @param userDF
	 * @param ratDF
	 */
	public static void popularBySql(SparkSession sparkSession, Dataset<Row> userDF, Dataset<Row> ratDF) {
		System.out.println("男性喜爱的10个电影 BySQL");
		System.out.println(new DateTime().toString("yyyy-MMM-dd HH:mm:ss:SSS"));// 2019-一月-03 20:10:05:305
		userDF.createOrReplaceTempView("t_user");
		ratDF.createOrReplaceTempView("t_rat");
		// 选择评论中是男性的评分
		Dataset<Row> sql = sparkSession.sql("select avg(rat) rat_avg ,MovieID from (" //
				+ "select r.* from t_rat r , t_user u where u.Gender='M' AND U.UserID = r.UserID )" + //
				"group by MovieID order by rat_avg desc limit 10");

		sql.show();
		System.out.println(new DateTime().toString("yyyy-MMM-dd HH:mm:ss:SSS"));
	}
}