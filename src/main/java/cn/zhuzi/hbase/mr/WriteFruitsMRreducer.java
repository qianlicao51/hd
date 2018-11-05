package cn.zhuzi.hbase.mr;

import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

/**
 * @Title: WriteFruitMRreducer.java
 * @Package cn.zhuzi.hbase.mr
 * @Description: TODO(hbase mr数据迁移 )
 * @author 作者 grq
 * @version 创建时间：2018年11月5日 上午11:19:48
 *
 */
public class WriteFruitsMRreducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
		// 读出来的每一行写入到fruit_mr
		for (Put put : values) {
			context.write(NullWritable.get(), put);
			System.out.println(ToStringBuilder.reflectionToString(put));
			System.out.println(ToStringBuilder.reflectionToString(NullWritable.get()));
			System.out.println();
		}
	}

}
