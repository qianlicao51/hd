package cn.zhuzi.sparksp02;

import java.io.IOException;
import java.util.List;

import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import cn.zhuzi.sparksp02.SparkSqlDemo.Person;

/**
 * @Title: SparkSqlDemo2.java
 * @Package cn.zhuzi.sparksp02
 * @Description: TODO(用一句话描述该文件做什么)
 * @author 作者 grq
 * @version 创建时间：2018年11月24日 下午5:25:13
 *
 */
public class SparkSqlDemo2 {
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

	public static void main(String[] args) {
		JavaRDD<Person> peRdd = sparkSession.read().textFile(base_path + "/per.txt").javaRDD().map(new Function<String, Person>() {
			@Override
			public Person call(String line) throws Exception {
				String[] parts = line.split(",");
				Person person = new Person();
				person.setName(parts[0]);
				person.setAge(Integer.parseInt(parts[1].trim()));
				return person;
			}
		});

		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		// TODO RDD 转DataFrame
		Dataset<Row> personDF = sparkSession.createDataFrame(peRdd, Person.class);
		// TODO DataFrame 转Dataset
		Dataset<Person> personDataset = personDF.as(personEncoder);
		personDataset.createOrReplaceTempView("person_dataset");
		Dataset<Row> result = sparkSession.sql("select * from person_dataset where age>20 ");
		Dataset<Person> resultDataset = result.as(personEncoder);
		List<Person> collectAsList = resultDataset.collectAsList();
		for (Person person : collectAsList) {
			System.out.println(person.toString());
		}
	}
}
