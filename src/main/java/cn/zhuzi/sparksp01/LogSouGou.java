package cn.zhuzi.sparksp01;

import java.io.File;
import java.io.IOException;

import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import cn.zhuzi.spark.SparkUtils;

public class LogSouGou {
	public static void main(String[] args) throws IOException {

		File logFile = Resources.getResourceAsFile("txt/SogouQ2.txt");
		JavaSparkContext context = SparkUtils.getContext();
		JavaRDD<String> javaRDD = context.textFile(logFile.getAbsolutePath());
		javaRDD.persist(new StorageLevel(true, true, false, true, 1));

		System.out.println(javaRDD.count());

		long count = javaRDD.filter(t -> t.contains("131")).count();
		System.out.println(count);
	}
}
