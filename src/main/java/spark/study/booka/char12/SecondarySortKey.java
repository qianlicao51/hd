/**  
 * All rights Reserved, Designed By grq
 * @Title:  SecondarySortKey.java   
 * @Package spark.study.booka.char12   
 * @Description:    TODO(排序 key)   
 * @author: grq  
 * @date:   2019年1月5日 下午2:31:43   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.booka.char12;

import java.io.Serializable;

import org.apache.commons.lang3.builder.CompareToBuilder;

public class SecondarySortKey implements Serializable, Comparable<SecondarySortKey> {

	private static final long serialVersionUID = 1L;
	private int first;
	private int second;

	/**
	 * @return first
	 */
	public int getFirst() {
		return first;
	}

	/**
	 * @param first 要设置的 first
	 */
	public void setFirst(int first) {
		this.first = first;
	}

	/**
	 * @return second
	 */
	public int getSecond() {
		return second;
	}

	/**
	 * @param second 要设置的 second
	 */
	public void setSecond(int second) {
		this.second = second;
	}

	/*
	 * （非 Javadoc）
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(SecondarySortKey o) {
		// 此结果是正序排列
		return new CompareToBuilder().append(first, o.first).append(second, o.second).toComparison();
		// 1::5
		// 2::4
		// 2::10
		// 3::6
	}

}
