package cn.sxt.day1.hdfs.wc;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.sxt.config.HadoopConfig;

public class MyWC {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(true);

		Job job = Job.getInstance(conf);

		job.setJarByClass(MyWC.class);

		// Specify various job-specific parameters
		job.setJobName("myjob_wc");
		FileInputFormat.addInputPath(job, new Path(HadoopConfig.inPath + "data/hello.txt"));
		FileOutputFormat.setOutputPath(job, new Path(HadoopConfig.getOutPath()));

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MyReducer.class);

		// Submit the job, then poll for progress until the job is complete
		job.waitForCompletion(true);
	}
}
