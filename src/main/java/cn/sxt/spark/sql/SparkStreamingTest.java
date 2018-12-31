/**  
 * All rights Reserved, Designed 
 * @Title:  SparkStreamingTest.java   
 * @Package cn.sxt.spark.sql   
 * @Description:    TODO(SparkStreaming demo)   
 * @author: grq  
 * @date:   2019年1月1日 上午1:37:45   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package cn.sxt.spark.sql;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * @author MI
 *
 */
public class SparkStreamingTest {
	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingTest");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");// 调日志爱级别
		JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));// 时间间隔

		// 在Linux上yum -y install cn ,使用 nc -lk 端口 ；发送数据
		JavaReceiverInputDStream<String> socketTextStream = jsc.socketTextStream("had2", 9999);
		JavaDStream<String> words = socketTextStream.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
		JavaPairDStream<String, Integer> reduceByKey = words.mapToPair(t -> new Tuple2<String, Integer>(t, 1)).reduceByKey((a, b) -> (a + b));

		// 打印前1000行
		reduceByKey.print(1000);

		/*
		 * foreachRDD可以拿到DStream中的RDD，对拿到的RDD可以使用RDD的transformation类算子转换，要
		 * 对拿到的RDD使用action算子触发执行，否则，foreacheRDD不会执行
		 */
		reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				System.out.println("Driver .......");
				SparkContext context = rdd.context();
				JavaSparkContext javaSparkContext = new JavaSparkContext(context);
				Broadcast<String> broadcast = javaSparkContext.broadcast("hello");
				String value = broadcast.value();
				System.out.println(value);
				JavaPairRDD<String, Integer> mapToPair = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
						System.out.println("Executor .......");
						return new Tuple2<String, Integer>(tuple._1 + "~", tuple._2);
					}
				});
				mapToPair.collect();// 没有这句话 下面不会输出
				mapToPair.foreach(new VoidFunction<Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Integer> arg0) throws Exception {
						System.out.println(arg0);
					}
				});
			}
		});

		jsc.start();
		jsc.awaitTermination();
		jsc.stop();
		jsc.close();
	}
}
