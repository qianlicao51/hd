package cn.sxt.day1.hdfs.fof;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.sxt.config.HadoopConfig;

public class MyFoF {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration(true);

		Job job = Job.getInstance(conf);
		job.setJarByClass(MyFoF.class);

		Path input = new Path(HadoopConfig.getInputPath("data/sxt/friend"));
		FileInputFormat.addInputPath(job, input);

		Path output = new Path(HadoopConfig.getOutPath());
		if (output.getFileSystem(conf).exists(output)) {
			output.getFileSystem(conf).delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(FofMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(FofReducer.class);

		job.waitForCompletion(true);
	}
}
