package cn.zhuzi.spark.chart10;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @Title: StreamDemo.java
 * @Package cn.zhuzi.spark.chart10
 * @Description: TODO(sparkStreaming_Demo)
 * @author 作者 grq
 * @version 创建时间：2018年11月22日 下午11:08:26
 *
 */
public class StreamDemo {
	public static void main(String[] args) {
		// Create the context with a 5 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("sparkStreaming_Demo").setMaster("local");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		System.out.println(ssc);
	}
}
