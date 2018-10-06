package cn.zhuzi.kafka.producer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @Title: CustomProducer1.java
 * @Package cn.zhuzi.kafka.producer
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年10月6日 下午7:48:35
 *
 */
public class CustomProducer1 {
	public static void main(String[] args) {
		Properties props = new Properties();
		// Kafka服务端的主机名和端口号
		props.put("bootstrap.servers", "hadoop101:9092");
		// 等待所有副本节点的应答
		props.put("acks", "all");
		// 消息发送最大尝试次数
		props.put("retries", 0);
		// 一批消息处理大小
		props.put("batch.size", 16384);
		// 增加服务端请求延时 单位ms
		props.put("linger.ms", 5);
		// 发送缓存区内存大小
		props.put("buffer.memory", 33554432);
		// key序列化
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// value序列化
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// props.put("partitioner.class",
		// "cn.zhuzi.kafka.partion.CustomPartion");

		List<String> inList = new ArrayList<String>();
		inList.add("cn.zhuzi.kafka.inter.TimeInterceptor");
		inList.add("cn.zhuzi.kafka.inter.CounterInterceptor");

		// 放入拦截器
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, inList);
		// 实例化
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 0; i < 20; i++) {

			producer.send(new ProducerRecord<String, String>("kindlebook", "java 发送" + i + "---" + new Date()), new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (metadata != null) {
						System.out.println(metadata.offset() + "--" + metadata.partition());
					}

				}
			});
		}
		producer.close();
	}
}
