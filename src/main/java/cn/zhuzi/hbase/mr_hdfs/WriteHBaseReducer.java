package cn.zhuzi.hbase.mr_hdfs;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Title: WriteHBaseReducer.java
 * @Package cn.zhuzi.hbase.mr_hdfs
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年11月5日 下午7:11:47
 *
 */
public class WriteHBaseReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
	@Override
	protected void reduce(ImmutableBytesWritable imm, Iterable<Put> puts, Reducer<ImmutableBytesWritable, Put, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
		// 读出来的每一行写入到 hbase表
		for (Put put : puts) {
			context.write(NullWritable.get(), put);
		}

	}
}
