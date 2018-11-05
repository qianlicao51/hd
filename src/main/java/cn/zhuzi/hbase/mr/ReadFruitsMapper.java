package cn.zhuzi.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Title: ReadFruitsMapper.java
 * @Package cn.zhuzi.hbase.mr
 * @Description: TODO(hbase mr数据迁移)
 * @author 作者 grq
 * @version 创建时间：2018年11月5日 上午11:07:09
 *
 */
public class ReadFruitsMapper extends TableMapper<ImmutableBytesWritable, Put> {

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {

		// 将fruits中数据提取出来，相当于将每一行数据读取出来放入到put对象中
		Put put = new Put(key.get());
		// 遍历添加column行
		for (Cell cell : value.rawCells()) {
			// 添加/克隆 列族 info
			if ("info".equals(org.apache.hadoop.hbase.util.Bytes.toString(CellUtil.cloneFamily(cell)))) {
				// 添加/克隆 列 name
				if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					// 将 该列加入到 put对象
					put.add(cell);

				} else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					// 向该列cell加入put对象重
					put.add(cell);
				}
			}
		}
		// 将fruit读取到每行数据写入到context 中 作为map输出
		context.write(key, put);
	}
}
