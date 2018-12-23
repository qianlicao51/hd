package cn.sxt.day1.hdfs.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class WeMapper extends Mapper<LongWritable, Text, Weath, IntWritable> {
	Weath we = new Weath();

	IntWritable mval = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 1949-10-01 14:21:02 34c
		String[] split = value.toString().split("\t");

		new DateTime().toString();
		DateTimeFormatter dFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
		DateTime dateTime = dFormat.parseDateTime(split[0]);
		we.setYear(dateTime.getYear());
		we.setMon(dateTime.getMonthOfYear());
		we.setDay(dateTime.getDayOfMonth());
		int wd = Integer.parseInt(split[1].substring(0, 2));
		we.setWd(wd);
		mval.set(wd);
		context.write(we, mval);

	}
}
