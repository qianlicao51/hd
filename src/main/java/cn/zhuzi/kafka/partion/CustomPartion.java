package cn.zhuzi.kafka.partion;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * @Title: CustomPartion.java
 * @Package cn.zhuzi.kafka.partion
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年10月6日 下午8:46:55
 *
 */
public class CustomPartion implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		return 5;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
