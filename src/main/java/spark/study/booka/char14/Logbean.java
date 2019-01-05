/**  
 * All rights Reserved, Designed By grq
 * @Title:  Logbean.java   
 * @Package spark.study.booka.char14   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: grq  
 * @date:   2019年1月5日 下午4:55:51   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.booka.char14;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @author MI
 *
 */
public class Logbean implements Serializable {
	private static final long serialVersionUID = 1L;
//{"logID": 0,"userID": 0, "time": "2016-10-04 15:42:45", "typed": 0, "consumed": 0.0}
	private Long logID;
	private Long userID;
	private Timestamp time;
	private int typed;
	private double consumed;

	/**
	 * @return logID
	 */
	public Long getLogID() {
		return logID;
	}

	/**
	 * @param logID 要设置的 logID
	 */
	public void setLogID(Long logID) {
		this.logID = logID;
	}

	/**
	 * @return userID
	 */
	public Long getUserID() {
		return userID;
	}

	/**
	 * @param userID 要设置的 userID
	 */
	public void setUserID(Long userID) {
		this.userID = userID;
	}

	/**
	 * @return time
	 */
	public Timestamp getTime() {
		return time;
	}

	/**
	 * @param time 要设置的 time
	 */
	public void setTime(Timestamp time) {
		this.time = time;
	}

	/**
	 * @return typed
	 */
	public int getTyped() {
		return typed;
	}

	/**
	 * @param typed 要设置的 typed
	 */
	public void setTyped(int typed) {
		this.typed = typed;
	}

	/**
	 * @return consumed
	 */
	public double getConsumed() {
		return consumed;
	}

	/**
	 * @param consumed 要设置的 consumed
	 */
	public void setConsumed(double consumed) {
		this.consumed = consumed;
	}

}
