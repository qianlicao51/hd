/**  
 * All rights Reserved, Designed By www.tydic.com
 * @Title:  SparkSqlDemo.java   
 * @Package cn.sxt.spark.sql   
 * @Description:    TODO(SparkSQL demo)   
 * @author: grq  
 * @date:   2018年12月31日 下午10:03:24   
 * @version V1.0 
 * @Copyright: 2018 grq All rights reserved. 
 */
package cn.sxt.spark.sql;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.io.Resources;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author MI
 *
 */
public class SparkSQLDemo {
	static SparkSession sparkSession;
	/**
	 * 数据文件位置
	 */
	static String base_path;
	static {
		SparkConf conf = new SparkConf();
		// 配置join或者聚合 操作shuffle数据时 分区数量
		conf.set("spark.sql.shuffle.partitions", "1");
		// spark 2.0 支持hive enableHiveSupport
		sparkSession = SparkSession.builder().appName("SparkSQL_demo").master("local")
				// .enableHiveSupport()
				.config(conf).getOrCreate();
		try {
			// 使用mybatis 获取资源文件路径
			base_path = Resources.getResourceAsFile("data/sxt/").getAbsolutePath();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		rowNumber();
	}

	/**
	 * UDF
	 */
	private static void demoUDF() {

	}

	/**
	 * 开窗函数row_number
	 */
	private static void rowNumber() {
		// 数据 data/sxt/row_number row_number 说明 日期 种类 销售额

		JavaRDD<String> javaRDD = sparkSession.sparkContext().textFile(base_path + "/row_number", 1).toJavaRDD();
		JavaRDD<Row> strRDD = javaRDD.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String v1) throws Exception {
				String[] split = v1.split(" ");
				return RowFactory.create(split[0], split[1], Integer.valueOf(split[2]));
			}
		});
		List<StructField> asList = Arrays.asList(//
				DataTypes.createStructField("date", DataTypes.StringType, true), //
				DataTypes.createStructField("clas", DataTypes.StringType, true), //
				DataTypes.createStructField("price", DataTypes.IntegerType, true));

		StructType structType = DataTypes.createStructType(asList);
		Dataset<Row> df = sparkSession.createDataFrame(strRDD, structType);
		df.createOrReplaceTempView("sales");
		/**
		 * 开窗函数 row_number;
		 * <p>
		 * 主要是按照某个字段分组，然后取另一字段的前几个值 ，相当于分组取topN
		 * <p>
		 * row_number() over (partition by xxx order by xxxx desc ) xxx
		 * <p>
		 * SparkSQL 如果使用了开窗函数，sql必须使用HiveContext(此处我使用的spark2 不适用enableHiveSupport
		 * 也能执行开窗函数)
		 */
		Dataset<Row> sql = sparkSession.sql("select date ,clas,price from ("//
				+ "select date ,clas ,price ,row_number() over (partition by clas order by price desc ) rank from sales"//
				+ ")t  where t.rank <=3 ");

		sql.show();

	}

	/**
	 * 从文本文件读取文件
	 */
	private static void readMysql() {
		Map<String, String> opations = new HashMap<String, String>();
		opations.put("url", "jdbc:mysql://localhost:3306/fm");//
		opations.put("driver", "com.mysql.jdbc.Driver");
		opations.put("password", "111111");
		opations.put("user", "root");
		opations.put("dbtable", "kindle_book");

		Dataset<Row> load = sparkSession.read().format("jdbc").options(opations).load();
		load.show();
		load.schema();

		Properties properties = new Properties();
		properties.setProperty("user", "root");
		properties.setProperty("password", "111111");
		Dataset<Row> jdbc = sparkSession.read().jdbc("jdbc:mysql://localhost:3306/fm", "kindle_book", properties);
		jdbc.show();
	}

	/**
	 * 文本文件读取数据第二种方式
	 */
	private static void readTxt2() {
		JavaRDD<String> lines = sparkSession.sparkContext().textFile(base_path + "/person.txt", 1).toJavaRDD();

		/**
		 * 转换成Row类型的RDD
		 */
		JavaRDD<Row> psersonRDD = lines.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String v1) throws Exception {
				String[] split = v1.split(",");
				return RowFactory.create(split[0], split[1], Integer.valueOf(split[2]));
			}
		});
		// name 类型 是否可以为空
		List<StructField> asList = Arrays.asList(DataTypes.createStructField("id", DataTypes.StringType, true), DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true));

		StructType structType = DataTypes.createStructType(asList);
		Dataset<Row> df = sparkSession.createDataFrame(psersonRDD, structType);
		df.printSchema();
		df.show();
		df.createOrReplaceTempView("per");
		sparkSession.sql("select name from per").show();
		/**
		 * 保存数据
		 */
		df.write().mode(SaveMode.Overwrite).parquet("./per");
		sparkSession.close();

	}

	/**
	 * 读取文本文件
	 */
	private static void readTxt() {
		JavaRDD<String> lines = sparkSession.sparkContext().textFile(base_path + "/person.txt", 1).toJavaRDD();
		JavaRDD<Person> psersonRDD = lines.map(new Function<String, Person>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Person call(String v1) throws Exception {
				// id name age
				Person person = new Person();
				String[] split = v1.split(",");
				person.setId(Integer.valueOf(split[0]));
				person.setName(split[1]);
				person.setAge(Integer.valueOf(split[2]));
				return person;
			}
		});
		/**
		 * 传入Person.class 的时候 。通过 的方式 创建Dataset()
		 */
		Dataset<Row> df = sparkSession.createDataFrame(psersonRDD, Person.class);
		df.printSchema();
		df.show();
		df.createOrReplaceTempView("per");
		sparkSession.sql("select name from per").show();
		sparkSession.close();
	}

}
