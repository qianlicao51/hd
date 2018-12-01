package cn.zhuzi.sparksp01;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.joda.time.DateTime;

import cn.zhuzi.spark.official.SparkUtils;
import scala.Tuple2;

/**
 * @Title: SouGou.java
 * @Package cn.zhuzi.sparksp01
 * @Description: TODO(搜狗查询日志排行)
 * @author 作者 grq
 * @version 创建时间：2018年11月21日 上午11:03:30
 *
 */
public class SouGou {
	static SparkSession sparkSession;
	static JavaSparkContext sc;
	static {
		if (sparkSession == null) {
			sparkSession = buildSparkSession();
			sc = new JavaSparkContext(sparkSession.sparkContext());
		}
	}
	static String textDataPath = "data/txt/207.TXT";

	public static void main(String[] args) throws IOException, AnalysisException {
		// sortBysearch();
		// sort();
		long startTime = System.currentTimeMillis();
		sortLogByKey();
		long second = System.currentTimeMillis();
		sortLogByKeyBySaprkSQLNoNeedBean();
		long endTime = System.currentTimeMillis();
		sortLogByKeyBySaprkSQL();
		long lastTime = System.currentTimeMillis();
		System.out.println("RDD使用时间(毫秒)是：" + (second - startTime));
		System.out.println("SparkSQL使用时间(毫秒)是" + (endTime - second));
		System.out.println("SparkSQL(javabean)使用时间" + (lastTime - endTime));
		// 使用时间
		// sortLogByKeyBySaprkSQL<sortLogByKeyBySaprkSQLNoNeedBean<sortLogByKey

	}

	@SuppressWarnings("serial")
	private static void sortLogByKeyBySaprkSQL() throws IOException, AnalysisException {
		String filePath = Resources.getResourceAsFile(textDataPath).getAbsolutePath();
		JavaRDD<LogBean> fileStrRdd = sc.textFile(filePath).map(new Function<String, LogBean>() {
			@Override
			public LogBean call(String v1) throws Exception {
				String[] splitFields = v1.split("\t");
				LogBean logBean = new LogBean();
				logBean.setDateStr(splitFields[0]);
				logBean.setUserid(splitFields[1]);
				logBean.setQueryWord(splitFields[2]);
				return logBean;
			}
		});
		Encoder<LogBean> logEncoder = Encoders.bean(LogBean.class);
		// TODO 读取文件转化为 DataSet
		Dataset<LogBean> logSouGouDataset = sparkSession.createDataFrame(fileStrRdd, LogBean.class).as(logEncoder);
		// TODO 使用全局 视图是 查询要带上global_temp
		// logSouGouDataset.createGlobalTempView("sougou_log");
		logSouGouDataset.createOrReplaceTempView("sougou_log");
		Dataset<Row> result = sparkSession.sql("select queryWord,count(queryWord) from sougou_log t group by queryWord order by count(queryWord) desc ,queryWord desc");
		File file = FileUtils.getFile("E:/had/spark/out/a_wcSaprkSQL" + new DateTime().toString("yyyyMMdd_HHmm_ss"));
		result.rdd().saveAsTextFile(file.toString());

	}

	@SuppressWarnings("serial")
	private static void sortLogByKeyBySaprkSQLNoNeedBean() throws IOException, AnalysisException {
		String filePath = Resources.getResourceAsFile(textDataPath).getAbsolutePath();
		// 20111230000005 57375476989eea12893c0c3811607bcf 奇艺高清 1 1
		// http://www.qiyi.com/
		String schameStr = "datestr,uuid,queryword,a,b,url";
		Dataset<Row> dateSet = SparkUtils.txtfileToDateSet(sparkSession, filePath, schameStr, "\t");
		// TODO 使用全局 视图是 查询要带上global_temp
		// logSouGouDataset.createGlobalTempView("sougou_log");
		dateSet.createOrReplaceTempView("sougou_log");
		Dataset<Row> result = sparkSession.sql("select queryWord,count(queryWord) from sougou_log t group by queryWord order by count(queryWord) desc ,queryWord desc");
		File file = FileUtils.getFile("E:/had/spark/out/a_wcSaprkSQLNoNeedBean" + new DateTime().toString("yyyyMMdd_HHmm_ss"));
		result.rdd().saveAsTextFile(file.toString());

	}

	/**
	 * 热搜词排序
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private static void sortLogByKey() throws IOException {

		String filePath = Resources.getResourceAsFile(textDataPath).getAbsolutePath();

		JavaRDD<String> fileStrRdd = sc.textFile(filePath);
		JavaRDD<String> filter = fileStrRdd.filter(t -> t.split("\t").length == 6);
		JavaPairRDD<String, Integer> mapToPair = filter.mapToPair(t -> (new Tuple2<String, Integer>((t.split("\t")[2]), 1)));
		JavaPairRDD<String, Integer> resuleRDD = mapToPair.reduceByKey((a, b) -> a + b).mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1)).sortByKey(false).mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1));
		resuleRDD.persist(StorageLevel.DISK_ONLY());
		List<Tuple2<String, Integer>> collect = resuleRDD.collect();
		File file = FileUtils.getFile("E:/had/spark/out/a_wc_rdd" + new DateTime().toString("yyyyMMdd_HHmm_ss"));
		resuleRDD.saveAsTextFile(file.getAbsolutePath());
	}

	/**
	 * 模拟wordcount 按照 词数量倒序排列
	 */
	public static void sortBysearch() {

		JavaRDD<String> lines = sc.parallelize(Arrays.asList("ahello", "bwod", "grq", "grq", "grq"));
		JavaPairRDD<String, Integer> reduceByKey = lines.mapToPair(t -> new Tuple2<String, Integer>(t, 1)).reduceByKey((a, b) -> a + b).sortByKey();
		System.out.println(reduceByKey.collect());
		// [(ahello,1), (bwod,1), (grq,3)]

		// TODO 此处排序，之后就不要排序了
		JavaPairRDD<Integer, String> secondStepRdd = reduceByKey.mapToPair(t -> new Tuple2<Integer, String>(t._2, t._1)).sortByKey(false);
		System.out.println(secondStepRdd.collect());
		// [(3,grq), (1,ahello), (1,bwod)]

		JavaPairRDD<String, Integer> resuleRdd = secondStepRdd.mapToPair(t -> new Tuple2<String, Integer>(t._2, t._1));
		System.out.println(resuleRdd.collect());
		// [(grq,3), (ahello,1), (bwod,1)]
	}

	/**
	 * 测试排序
	 */
	public static void sort() {
		List<Integer> takeOrdered = sc.parallelize(Arrays.asList(2, 3, 4, 5, 6)).takeOrdered(2);
		System.out.println(takeOrdered);

	}

	/**
	 * 官方例子构建session的方法
	 */
	public static SparkSession buildSparkSession() {
		// TODO spark.sql.shuffle.partitions 设置并行度，不设置的话SparkSQL查询保存会保存多个文件
		SparkSession sparkSession = SparkSession.builder().appName("JavaSparkPi").config("spark.sql.shuffle.partitions", 1)
		// .master("spark://hadoop:7077")远程地址
				.master("local").getOrCreate();
		return sparkSession;
	}

}
