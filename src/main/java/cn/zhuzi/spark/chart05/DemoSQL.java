package cn.zhuzi.spark.chart05;

import org.apache.hadoop.hive.ql.parse.HiveParser.statement_return;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

import scala.Function0;
import cn.zhuzi.spark.SparkUtils;

/**
 * @Title: DemoSQL.java
 * @Package cn.zhuzi.spark.chart05
 * @Description: TODO(spark sql连接 )
 * @author 作者 grq
 * @version 创建时间：2018年11月16日 下午9:10:35
 *
 */
public class DemoSQL {
	/**
	 * Java创建hiveContext并查询数据
	 */
	public static void connHive() {
		// 我使用到这个是spark 2.1.1版本，HiveContext已经过时了
		HiveContext hiveContext = new HiveContext(SparkUtils.getContext());
		Dataset<Row> rows = hiveContext.sql("select name,age from user");
		Row first = rows.first();
		System.out.println(first.getString(0));// 字段0是name字段
	}

	/**
	 * Java中使用Spark SQL 读取JSON数据
	 */
	public static void sparkSQLToJson() {
		HiveContext hiveCtx = new HiveContext(SparkUtils.getContext());
		Dataset<Row> jsonFile = hiveCtx.jsonFile("tweets.json");
		jsonFile.registerTempTable("");

	}
	public static void connMySQL(){
	}
}
