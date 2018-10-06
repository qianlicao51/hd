package cn.zhuzi.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @Title: CustomConsumer.java
 * @Package cn.zhuzi.kafka.consumer
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年10月6日 下午8:51:31
 *
 */
public class CustomConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		// 定义kakfa 服务的地址，不需要将所有broker指定上
		props.put("bootstrap.servers", "hadoop101:9092");
		// 制定consumer group
		props.put("group.id", "g1");
		// 是否自动确认offset
		props.put("enable.auto.commit", "true");
		// 自动确认offset的时间间隔
		props.put("auto.commit.interval.ms", "1000");
		// key的序列化类
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// value的序列化类
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		// 释放资源
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				if (consumer != null) {
					consumer.close();
				}
			}
		}));

		// 消费者订阅的topic, 可同时订阅多个
		consumer.subscribe(Arrays.asList("kindlebook"));
		while (true) {
			// 读取数据，读取超时时间为100ms
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		}
	}
}
