/**  
 * All rights Reserved, Designed By www.tydic.com
 * @Title:  Person.java   
 * @Package cn.sxt.spark.sql   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: grq  
 * @date:   2018年12月31日 下午10:18:17   
 * @version V1.0 
 * @Copyright: 2018 grq All rights reserved. 
 */
package cn.sxt.spark.sql;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * @author MI
 *
 */
public class Person implements Serializable {

	private static final long serialVersionUID = 1L;
	private int id;
	private int age;
	private String name;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
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