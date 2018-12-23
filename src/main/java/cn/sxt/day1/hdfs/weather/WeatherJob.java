package cn.sxt.day1.hdfs.weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.sxt.config.HadoopConfig;
import cn.sxt.day1.hdfs.wc.MyWC;

/**
 * 天 气 找出气温最高的2天
 * 
 * @author MI
 *
 */
public class WeatherJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(true);

		Job job = Job.getInstance(conf);

		job.setJarByClass(MyWC.class);

		// Specify various job-specific parameters
		job.setJobName("myjob_weather");

		// -- map
		job.setMapperClass(WeMapper.class);
		job.setMapOutputKeyClass(Weath.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setPartitionerClass(WePartitioner.class);
		job.setSortComparatorClass(WeSort.class);

		// --redce
		job.setGroupingComparatorClass(WeGroupingComparator.class);
		job.setReducerClass(WeReducer.class);

		FileInputFormat.addInputPath(job, new Path(HadoopConfig.getInputPath("data/sxt/tq")));
		FileOutputFormat.setOutputPath(job, new Path(HadoopConfig.getOutPath()));

		job.setNumReduceTasks(2);
		// Submit the job, then poll for progress until the job is complete
		job.waitForCompletion(true);

	}
}
