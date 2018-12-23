package cn.sxt.day1.hdfs.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeReducer extends Reducer<Weath, IntWritable, Text, IntWritable> {
	Text rkey = new Text();
	IntWritable rval = new IntWritable();

	@Override
	protected void reduce(Weath key, Iterable<IntWritable> val, Context con) throws IOException, InterruptedException {

		int flag = 0;
		int day = 0;
		for (IntWritable v : val) {
			System.out.println(key + "<>" + v);
			if (flag == 0) {
				day = key.getDay();
				rkey.set(key.getYear() + "-" + key.getMon() + "-" + key.getDay());
				rval.set(key.getWd());
				con.write(rkey, rval);
				flag++;

			}
			if (flag != 0 && day != key.getDay()) {
				rkey.set(key.getYear() + "-" + key.getMon() + "-" + key.getDay());
				rval.set(key.getWd());
				con.write(rkey, rval);
				break;
			}
		}
		System.out.println("-----------"  );
	}
}
