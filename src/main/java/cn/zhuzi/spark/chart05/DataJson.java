package cn.zhuzi.spark.chart05;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import cn.zhuzi.spark.SparkUtils;

import com.alibaba.fastjson.JSON;

/**
 * @Title: DataJson.java
 * @Package cn.zhuzi.spark.chart05
 * @Description: TODO(Spark 读取JSON )
 * @author 作者 grq
 * @version 创建时间：2018年11月16日 下午8:36:04
 *
 */
public class DataJson {

	public static void main(String[] args) {
		readJson();
	}

	private static void readJson() {

		JavaSparkContext sc = SparkUtils.getContext();
		JavaRDD<String> input = sc.textFile("path");
	}
}

class PersonJson implements FlatMapFunction<Iterable<String>, Person> {
	ArrayList<Person> persons = new ArrayList<Person>();

	@Override
	public Iterator<Person> call(Iterable<String> lines) throws Exception {
		for (String str : lines) {
			Person per = JSON.parseObject(str, Person.class);
			persons.add(per);
		}
		return (Iterator<Person>) persons;
	}
}

class Person {
	private String id;
	private String name;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
