package cn.zhuzi.kafka.inter;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @Title: CounterInterceptor.java
 * @Package cn.zhuzi.kafka.inter
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年10月6日 下午9:29:50
 *
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

	private long successCount = 0;
	private long errorCount = 0;

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		if (exception == null) {
			successCount++;
		} else {
			errorCount++;
		}
	}

	@Override
	public void close() {
		System.out.println("success:" + successCount);
		System.out.println("error:" + errorCount);
	}

}
