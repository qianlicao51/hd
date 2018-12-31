/**  
 * All rights Reserved, Designed By grq
 * @Title:  SQLWithStreaming.java   
 * @Package cn.sxt.spark.sql   
 * @Description:    TODO(spark steaming 中使用sparksql查询)   
 * @author: grq  
 * @date:   2019年1月1日 下午3:16:10   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package cn.sxt.spark.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author MI
 *
 */
public class SQLWithStreaming {
	public static void main(String[] args) throws InterruptedException {

		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SQLwithStreaming");
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		jsc.checkpoint("./checkpoint");

		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("had2", 9999);// linux nc -lk 9999

		JavaDStream<String> words = lines.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
		/**
		 * 每30 秒计算最近1分钟的数据量
		 */

		JavaDStream<String> window = words.window(Durations.minutes(1), Durations.seconds(30));

		window.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> javaRDD) throws Exception {
				JavaRDD<Row> wordRowRDD = javaRDD.map(t -> RowFactory.create(t));
				SparkSession sparkSession = SparkSession.builder().getOrCreate();
				/**
				 * 创建schema
				 */
				List<StructField> fields = new ArrayList<StructField>();
				fields.add(DataTypes.createStructField("word", DataTypes.StringType, true));
				StructType createStructType = DataTypes.createStructType(fields);
				Dataset<Row> allwords = sparkSession.createDataFrame(wordRowRDD, createStructType);
				// allwords.show();// 显示打印
				allwords.createOrReplaceTempView("tmp_word");

				Dataset<Row> sql = sparkSession.sql("select word ,count(word) rank from tmp_word group by word order by rank desc ");
				sql.show(1000);

			}
		});
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
}
