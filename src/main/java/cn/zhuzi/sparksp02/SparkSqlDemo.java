package cn.zhuzi.sparksp02;

//静态导入
import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @Title: SparkSqlDemo.java
 * @Package cn.zhuzi.sparksp02
 * @Description: TODO(SparkSQL)
 * @author 作者 grq
 * @version 创建时间：2018年11月22日 下午8:28:33
 *
 */
public class SparkSqlDemo {
	static SparkSession sparkSession;
	/**
	 * 数据文件位置
	 */
	static String base_path;
	static {
		sparkSession = SparkSession.builder().appName("SparkSQL_demo").master("local").getOrCreate();
		try {
			/**
			 * 使用mybatis 获取资源文件路径
			 */
			base_path = Resources.getResourceAsFile("data/official/").getAbsolutePath();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws AnalysisException {
		readJsonRunBasicDataFrame(sparkSession);
		readJsonRunBasicDataSet(sparkSession);
		readJsonInferSchema(sparkSession);
		readJSONProgrammatic(sparkSession);
		fun(sparkSession);
		// TODO SparkSQL 转化为 DataSet更加方便，因为它对字段是类型检查的
	}

	private static void readJSONProgrammatic(SparkSession spark) {
		JavaRDD<String> javaRDD = spark.sparkContext().textFile(base_path + "/per.txt", 1).toJavaRDD();
		String schemaString = "name age";
		List<StructField> fields = new ArrayList<StructField>(16);
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = javaRDD.map(new Function<String, Row>() {
			@Override
			public Row call(String record) throws Exception {
				String[] attributes = record.split(",");
				return RowFactory.create(attributes[0], attributes[1].trim());
			}
		});

		Dataset<Row> createDataFrame = spark.createDataFrame(rowRDD, schema);

	}

	private static void readJsonInferSchema(SparkSession spark) {

		// TODO txt文件这样会报错
		// Dataset<Person> as = spark.read().text(base_path +
		// "/per.txt").as(personEncoder);
		JavaRDD<Person> peRdd = spark.read().textFile(base_path + "/per.txt").javaRDD().map(new Function<String, Person>() {
			@Override
			public Person call(String line) throws Exception {
				String[] parts = line.split(",");
				Person person = new Person();
				person.setName(parts[0]);
				person.setAge(Integer.parseInt(parts[1].trim()));
				return person;
			}
		});
		Dataset<Row> personDF = spark.createDataFrame(peRdd, Person.class);
		personDF.show();
		personDF.createOrReplaceTempView("per");
		Dataset<Row> dataset = spark.sql("select name from per where age >2");

		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> nameDF = dataset.map(new MapFunction<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return "Name: " + row.getString(0);
			}
		}, stringEncoder);
		nameDF.show();
		// +-------------+
		// | value|
		// +-------------+
		// |Name: Michael|
		// | Name: Andy|
		// | Name: Justin|
		// +-------------+
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> asDataset = personDF.as(personEncoder);
		asDataset.show();

	}

	/**
	 * 读取json文件为 Dataset
	 * 
	 * @param sparkSession
	 * @throws AnalysisException
	 */
	public static void readJsonRunBasicDataFrame(SparkSession sparkSession) throws AnalysisException {
		Dataset<Row> df = sparkSession.read().json(base_path + "/people.json");
		df.show();
		// +----+-------+
		// | age| name|
		// +----+-------+
		// |null|Michael|
		// | 30| Andy|
		// | 19| Justin|
		// +----+-------+
		df.printSchema();
		// root
		// |-- age: long (nullable = true)
		// |-- name: string (nullable = true)
		// TODO 静态导入import static org.apache.spark.sql.functions.col;
		df.select(org.apache.spark.sql.functions.col("name"), col("age").plus(1)).show();
		// +-------+---------+
		// | name|(age + 1)|
		// +-------+---------+
		// |Michael| null|
		// | Andy| 31|
		// | Justin| 20|
		// +-------+---------+
		// 年龄大于21
		df.filter(col("age").gt(21)).show();
		// +---+----+
		// |age|name|
		// +---+----+
		// | 30|Andy|
		// +---+----+
		df.groupBy("age").count().show();
		// +----+-----+
		// | age|count|
		// +----+-----+
		// | 19| 1|
		// |null| 1|
		// | 30| 1|
		// +----+-----+
		df.createOrReplaceTempView("per_temp");
		Dataset<Row> sql = sparkSession.sql("select * from per_temp");
		sql.show();

		// TODO 全局名称查询需要使用 global_temp
		df.createGlobalTempView("per_glo");
		sparkSession.sql("SELECT * FROM global_temp.per_glo").show();

	}

	public static class Person implements Serializable {
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this);
		}

	}

	public static void readJsonRunBasicDataSet(SparkSession spark) {
		Person person = new Person();
		person.setAge(25);
		person.setName("DataSet_demo");
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeansDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		javaBeansDS.show();
		// +---+------------+
		// |age| name|
		// +---+------------+
		// | 25|DataSet_demo|
		// +---+------------+

		Encoder<Integer> intEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), intEncoder);

		Dataset<Integer> transformDS = primitiveDS.map((t -> t + 1), intEncoder);
		System.out.println(transformDS.collectAsList());// [2, 3, 4]

		Dataset<Person> df = sparkSession.read().json(base_path + "/people.json").as(personEncoder);
		df.show();
		// +----+-------+
		// | age| name|
		// +----+-------+
		// |null|Michael|
		// | 30| Andy|
		// | 19| Justin|
		// +----+-------+
	}

	/**
	 * 测试sql函数
	 * 
	 * @param sparkSession
	 */
	public static void fun(SparkSession sparkSession) {
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> df = sparkSession.read().json(base_path + "/people.json").as(personEncoder);
		df.createOrReplaceTempView("per_fun");

		sparkSession.sql("select avg(age) from per_fun").show();
	}
}
