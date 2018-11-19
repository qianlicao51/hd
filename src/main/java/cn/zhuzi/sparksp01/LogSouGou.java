package cn.zhuzi.sparksp01;

import java.io.File;
import java.io.IOException;

import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import cn.zhuzi.spark.SparkUtils;

public class LogSouGou {
	public static void main(String[] args) throws IOException {

		File logFile = Resources.getResourceAsFile("txt/SogouQ2.txt");
		JavaSparkContext context = SparkUtils.getContext();

		JavaRDD<String> javaRDD = context.textFile(logFile.getAbsolutePath());

		System.out.println(javaRDD.count());
	}
}
