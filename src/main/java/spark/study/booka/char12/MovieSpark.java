/**  
 * All rights Reserved, Designed By grq
 * @Title:  MovieSpark.java   
 * @Package spark.study.booka.char12   
 * @Description:    TODO(电影推荐)   
 * @author: grq  
 * @date:   2019年1月2日 下午8:10:19   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.booka.char12;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.netty.handler.logging.LogLevel;

/**
 * @author MI
 *
 */
public class MovieSpark {
	static JavaSparkContext sc;
	static SparkSession sparkSession;
	static String base_path = null;

	static {
		sparkSession = SparkSession.builder().appName("JavaSparkPi").master("local").getOrCreate();
		sc = new JavaSparkContext(sparkSession.sparkContext());
		/**
		 * 使用mybatis 获取资源文件路径
		 */
		try {
			base_path = Resources.getResourceAsFile("data/spark/movie").getAbsolutePath();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		sc.setLogLevel(LogLevel.WARN.toString());// 日志级别
		JavaRDD<String> userRdd = sc.textFile(base_path + "/users.dat");
		JavaRDD<String> ratRdd = sc.textFile(base_path + "/ratings.dat");
		JavaRDD<String> occuRdd = sc.textFile(base_path + "/Occupation.dat");
		JavaRDD<String> moviesRdd = sc.textFile(base_path + "/movies.dat");

		/**
		 * 用户
		 */
		List<StructField> asListUser = Arrays.asList(//
				DataTypes.createStructField("UserID", DataTypes.StringType, true), //
				DataTypes.createStructField("Gender", DataTypes.StringType, true), //
				DataTypes.createStructField("Age", DataTypes.StringType, true), //
				DataTypes.createStructField("OccupationID", DataTypes.StringType, true), //
				DataTypes.createStructField("Zip-code", DataTypes.StringType, true)

		);
		Dataset<Row> userDF = rowToDf(userRdd, DataTypes.createStructType(asListUser));
		userDF.createOrReplaceTempView("t_user");

		/**
		 * ratRdd
		 */
		List<StructField> asListRat = Arrays.asList(//
				DataTypes.createStructField("UserID", DataTypes.StringType, true), //
				DataTypes.createStructField("MovieID", DataTypes.StringType, true), //
				DataTypes.createStructField("rat", DataTypes.IntegerType, true), //
				DataTypes.createStructField("Timestamp", DataTypes.StringType, true));
		JavaRDD<Row> ratRdd2 = ratRdd.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String v1) throws Exception {
				String[] split = v1.split("::");
				return RowFactory.create(split[0], split[1], Integer.valueOf(split[2]), split[3]);
			}
		});
		Dataset<Row> ratDF = sparkSession.createDataFrame(ratRdd2, DataTypes.createStructType(asListRat));
		ratDF.createOrReplaceTempView("t_rat");
		/**
		 * movie
		 */

		List<StructField> asListMovie = Arrays.asList(//
				DataTypes.createStructField("MovieID", DataTypes.StringType, true), //
				DataTypes.createStructField("Title", DataTypes.StringType, true), //
				DataTypes.createStructField("Genres", DataTypes.StringType, true));
		Dataset<Row> movieDF = rowToDf(moviesRdd, DataTypes.createStructType(asListMovie));
		movieDF.createOrReplaceTempView("t_mov");

		/**
		 * 职业Occupation OccupationID::OccupationName
		 */

		List<StructField> asListOcc = Arrays.asList(//
				DataTypes.createStructField("OccupationID", DataTypes.StringType, true), //
				DataTypes.createStructField("OccupationName", DataTypes.StringType, true));
		Dataset<Row> occDF = rowToDf(moviesRdd, DataTypes.createStructType(asListOcc));
		occDF.createOrReplaceTempView("t_occ");

		// TODO 1:RDD实现电影流行度 (1):所有电影中平均得分最高的Top10电影 和 所有电影粉丝最多的电影

		MovieAnalysis.rddForMovieTop10(sparkSession, ratRdd);
		MovieAnalysis.rddForMovieTop10Bysql(sparkSession, ratDF);
		sc.close();
	}

	/**
	 * @param rrdd
	 * @param createStructType
	 * @return
	 */
	private static Dataset<Row> rowToDf(JavaRDD<String> rdd, StructType structType) {
		return sparkSession.createDataFrame(rdd.map(t -> RowFactory.create(t.split("::"))), structType);
	}

}
