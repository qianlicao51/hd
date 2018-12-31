/**  
 * All rights Reserved, Designed By grq
 * @Title:  WordCountOnline.java   
 * @Package cn.sxt.spark.sql   
 * @Description:    TODO(streaming word count)   
 * @author: grq  
 * @date:   2019年1月1日 下午3:40:31   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package cn.sxt.spark.sql;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import cn.zhuzi.spark.official.SparkUtils;

/**
 * 模拟在线统计每次输入的单词个数
 *
 */
public class WordCountOnline {
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCountOnline");
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		JavaReceiverInputDStream<String> lines = jsc.socketTextStream("had2", 9999);
		JavaDStream<String> wordDstream = lines.flatMap(t -> Arrays.asList(t.split(" ")).iterator());// .map(t -> new Tuple2<String, Integer>(t, 1));
		// 这部分还需要继续看资料
		jsc.start();
		jsc.awaitTermination();
		/**
		 * JavaStreamingContext.stop()无参的stop方法会将sparkContext一同关闭，stop(false)
		 */
		jsc.stop(false);
		jsc.close();
		
		SparkSession sparkSession = SparkUtils.buildSparkSession();
		sparkSession.readStream().format("");
	}
}
