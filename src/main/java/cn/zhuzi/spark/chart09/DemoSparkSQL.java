package cn.zhuzi.spark.chart09;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
//不能使用hive时，导入Spark SQL
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
//导入Spark SQL
import org.apache.spark.sql.hive.HiveContext;

import scala.collection.immutable.Map;
import cn.zhuzi.spark.SparkUtils;

/**
 * @Title: Demo.java
 * @Package cn.zhuzi.spark.chart09
 * @Description: TODO(Spark SQL)
 * @author 作者 grq
 * @version 创建时间：2018年11月17日 上午12:13:13
 *
 */
public class DemoSparkSQL {

	/**
	 * 初始化SparkSQL
	 * <p>
	 * 书上介绍的api,我使用2.1.1是过时了
	 */
	public static SQLContext getSparkSQL() {
		JavaSparkContext sparkContext = SparkUtils.getContext();
		HiveContext hiveContext = new HiveContext(sparkContext);
		SQLContext sqlContext = new SQLContext(sparkContext);
		return sqlContext;
	}

	/**
	 * 查询
	 * 
	 * @throws IOException
	 */
	static void selectBySparkSQL() throws IOException {
		SQLContext sparkSQL = getSparkSQL();
		Dataset<Row> input = sparkSQL.jsonFile(Resources.getResourceAsFile("json/person.json").getAbsolutePath());
		// 注册输入的 内容 (提示次方法过时了)
		input.registerTempTable("per_temp");
		Dataset<Row> resultDataset = sparkSQL.sql("select name,gender,sal from per_temp order by sal limit 2");
		List<Row> collectAsList = resultDataset.collectAsList();
		for (Row row : collectAsList) {
			// System.out.println(ToStringBuilder.reflectionToString(row));
			System.out.println("名字：" + row.get(0) + "  ,sal:" + row.get(2));
		}

	}

	/**
	 * 查询(新版本API)
	 * 
	 * @throws IOException
	 */
	static void selectBySparkSQL2() throws IOException {
		SparkSession sparkSession = SparkUtils.buildSparkSession();
		Dataset<Row> json = sparkSession.read().json(Resources.getResourceAsFile("json/person.json").getAbsolutePath());
		SQLContext sqlContext = json.sqlContext();
		sqlContext.registerDataFrameAsTable(json, "per_temp");
		Dataset<Row> sql = sqlContext.sql("select * from per_temp order by sal limit 2");
		Dataset<Row> limit = json.orderBy("sal").limit(2);
		System.out.println(limit);
		System.out.println(sql);
	}

	/*
	 * apache Hive
	 */
	static void hive() {
		JavaSparkContext sc = SparkUtils.getContext();
		HiveContext hiveContext = new HiveContext(sc);
		Dataset<Row> rows = hiveContext.sql("SELECT KEY,VALUE FROM MyTable");
		JavaRDD<Object> map = rows.toJavaRDD().map(r -> r.get(0));
	}

	static void hive2() {
		SparkSession sparkSession = SparkUtils.buildSparkSession();
		Dataset<Row> sql = sparkSession.sql("SELECT KEY,VALUE FROM MyTable");
		JavaRDD<Object> map = sql.toJavaRDD().map(r -> r.get(0));
	}

	public static void main(String[] args) throws IOException {
		// selectBySparkSQL2();

		SparkSession session = buildSparkSession();
		Map<String, String> all = session.conf().getAll();
		

	}

	/**
	 * 2.0版本创建sparkSession
	 */
	public static SparkSession buildSparkSession() {
		SparkSession sparkSession = SparkSession.builder().appName("MyLocal").master("local").config("key", "value").getOrCreate();
		return sparkSession;
	}

	/**
	 * 2.0版本创建支持hive的sparkSession
	 */
	public static SparkSession buildSparkSessionEnableHive() {
		SparkSession sparkSession = SparkSession.builder().appName("MyLocal").master("local").config("key", "value").enableHiveSupport().getOrCreate();
		return sparkSession;
	}
}
