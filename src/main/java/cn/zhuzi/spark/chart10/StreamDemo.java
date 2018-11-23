package cn.zhuzi.spark.chart10;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
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
	public static void main(String[] args) throws InterruptedException {

		errorDemo(createStreamingContext());

	}

	/**
	 * 创建 JavaStreamingContext
	 * 
	 * @returnJavaStreamingContext
	 */
	private static JavaStreamingContext createStreamingContext() {
		// Create the context with a 5 second batch size
		// TODO 此处 创建一个拥有两个线程的应用程序 local[2] 一定是2个否则看不到效果
		SparkConf sparkConf = new SparkConf().setAppName("sparkStreaming_Demo").setMaster("local[2]");
		return new JavaStreamingContext(sparkConf, Durations.seconds(5));
	}

	/**
	 * 打印包含error的行 <br>
	 * 需要注意的是配置一个拥有两个线程的应用程序 local[2]<br>
	 * nc 模拟数据 先启动发送端，<br>
	 * 
	 * nc 在windows 发送端: nc -l -p 端口 <br>
	 * nc 在windows 接收端: nc 127.0.0.1 端口 <br>
	 * 
	 * @param ssc
	 */
	private static void errorDemo(JavaStreamingContext ssc) throws InterruptedException {
		// 以8888 端口作为输入创建DStream
		JavaReceiverInputDStream<String> socketTextStream = ssc.socketTextStream("127.0.0.1", 8888);
		// 从中筛选出包含error的行

		// JavaDStream<String> javaDStream =
		// socketTextStream.filter(t->t.contains("error"));
		// 打印有error的行
		JavaDStream<String> dStream = socketTextStream.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {
				return v1.contains("error");
			}
		});
		dStream.print();
		// 启动流计算环境
		ssc.start();
		// 等待作业完成
		ssc.awaitTermination();
	}
}
