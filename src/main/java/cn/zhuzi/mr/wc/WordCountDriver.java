package cn.zhuzi.mr.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author 作者 grq
 * @version 创建时间：2018年6月29日 下午4:09:54
 *
 */
public class WordCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		// 获取JOB对象
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 2 加载驱动类
		job.setJarByClass(WordCountDriver.class);
		// 3 加载mapper和reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		// 3 map端输出key 和 values
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 4 最终输出key 和values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 5 指定 输入 文件路劲
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 6 指定输出文件
		// 7 提交
		job.submit();
		boolean forCompletion = job.waitForCompletion(true);
		System.exit(forCompletion?0:1);
		
		
	}
	
	
	
}
