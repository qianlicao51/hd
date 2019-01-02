/**  
 * All rights Reserved, Designed By grq
 * @Title:  Demo01.java   
 * @Package cn.java8.char05   
 * @Description:    TODO(java 8实战 第五章)   
 * @author: grq  
 * @date:   2019年1月1日 下午11:16:59   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package cn.java8.char05;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author MI
 *
 */
public class Demo01 {
	public static void main(String[] args) {

		List<String> asList = Arrays.asList("hello", "world");
		List<String[]> collect = asList.stream().map(t -> t.split(""))//
				.distinct().collect(Collectors.toList());

		System.out.println(collect.size());

		String[] arrayOfWprds = { "hello", "world" };
		Stream<String> stream = Arrays.stream(arrayOfWprds);
		System.out.println(stream.collect(Collectors.toList()));// [hello, world]
		System.out.println("-------------------------");

		/**
		 * 次方法得到的是一个流列表
		 */
		List<Stream<String>> collect2 = asList.stream().map(t -> t.split(""))// 将每个单词转换为由其字母构成的数组
				.map(Arrays::stream)// 让每个数组变成一个单独的流
				.distinct()//
				.collect(Collectors.toList());

		/**
		 * 使用flatMap 的到[h, e, l, o, w, r, d]
		 */
		List<String> collect3 = asList.stream()//
				.map(t -> t.split(""))// 将每个单词转换为由字母构成的数组
				.flatMap(Arrays::stream)// 将各个生成流扁平化为单个流
				.distinct().collect(Collectors.toList());
		System.out.println(collect3);

	}
}
