/**  
 * All rights Reserved, Designed By grq
 * @Title:  SecondarySort.java   
 * @Package spark.study.booka.char12   
 * @Description:    TODO(二次排序)   
 * @author: grq  
 * @date:   2019年1月5日 下午2:26:35   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.booka.char12;

import java.io.IOException;

import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import io.netty.handler.logging.LogLevel;
import scala.Tuple2;

/**
 * @author MI
 *
 */
public class SecondarySort {
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
		JavaRDD<String> sortRdd = sc.textFile(base_path + "/secondsort");

		sortRdd.cache();
		JavaPairRDD<SecondarySortKey, String> mapToPair = sortRdd.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<SecondarySortKey, String> call(String t) throws Exception {
				SecondarySortKey secondarySortKey = new SecondarySortKey();
				secondarySortKey.setFirst(Integer.valueOf(t.split("::")[0]));
				secondarySortKey.setSecond(Integer.valueOf(t.split("::")[1]));
				return new Tuple2<SecondarySortKey, String>(secondarySortKey, t);
			}
		});

		// 上面是按照key正序排列的 。sortByKey(false) 会使结果倒序排列
		JavaPairRDD<SecondarySortKey, String> sortByKey = mapToPair.sortByKey(false);
		sortByKey.foreach(t -> System.out.println(t._2));
	}
}
