/**  
 * All rights Reserved, Designed By grq
 * @Title:  Dish.java   
 * @Package cn.java8.char04   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: grq  
 * @date:   2019年1月1日 下午6:40:10   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package cn.java8.char04;

/**
 * @author MI
 *
 */
public class Dish {
	private final String name;
	private final boolean vegetarian;
	private final int calories;
	private final Type type;

	public Dish(String name, boolean vegetarian, int calories, Type type) {
		this.name = name;
		this.vegetarian = vegetarian;
		this.calories = calories;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public boolean isVegetarian() {
		return vegetarian;
	}

	public int getCalories() {
		return calories;
	}

	public Type getType() {
		return type;
	}

	@Override
	public String toString() {
		return name;
	}

	public enum Type {
		MEAT, FISH, OTHER
	}
}
