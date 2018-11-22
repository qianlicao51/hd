package cn.zhuzi.spark.chart09;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Title: SchemaRDDDemo.java
 * @Package cn.zhuzi.spark.chart09
 * @Description: TODO(基于 RDD 创建 SchemaRDD)
 * @author 作者 grq
 * @version 创建时间：2018年11月20日 下午4:15:21
 *
 */
public class SchemaRDDDemo {
	public static class Person implements Serializable {
		private static final long serialVersionUID = 1L;
		private String name;
		private Integer age;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public Integer getAge() {
			return age;
		}
		public void setAge(Integer age) {
			this.age = age;
		}
		public Person(String name, Integer age) {
			this.name = name;
			this.age = age;
		}
	}

	public static void main(String[] args) {
		ArrayList<Person> perList = new ArrayList<SchemaRDDDemo.Person>(16);
		perList.add(new Person("demo", 26));
		perList.add(new Person("grq", 25));
		long startTimes = System.currentTimeMillis();
		SparkSession spark = SparkSession.builder().appName("MySpark").master("local[1]").enableHiveSupport().getOrCreate();
		Dataset<Row> recordsDF = spark.createDataFrame(perList, Person.class);
		recordsDF.createOrReplaceTempView("person_table");
		spark.sql("SELECT * FROM person_table t where t.age='25'").show();
		long endTimes = System.currentTimeMillis();//
		System.out.println(endTimes - startTimes);
	}
}
