package cn.zhuzi.sparksql;

import java.io.Serializable;

/**
 * @Title: Stock.java
 * @Package cn.zhuzi.sparksql
 * @Description: TODO(订单表头)
 * @author 作者 grq
 * @version 创建时间：2018年11月25日 上午1:30:06 订单号，交易位置，交易日期 BYSL00000893,ZHAO,2007-8-23
 * 
 *          CREATE TABLE tblStock(ordernumber STRING,locationid STRING,dateID
 *          string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES
 *          TERMINATED BY '\n'
 */
public class Stock implements Serializable {

	private static final long serialVersionUID = 1L;
	/**
	 * 订单号
	 */
	private String ordernumber;
	/**
	 * 交易位置
	 */
	private String locationid;
	/**
	 * 交易日期
	 */
	private String dateID;

	public String getOrdernumber() {
		return ordernumber;
	}

	public void setOrdernumber(String ordernumber) {
		this.ordernumber = ordernumber;
	}

	public String getLocationid() {
		return locationid;
	}

	public void setLocationid(String locationid) {
		this.locationid = locationid;
	}

	public String getDateID() {
		return dateID;
	}

	public void setDateID(String dateID) {
		this.dateID = dateID;
	}

	@Override
	public String toString() {
		return "Stock [ordernumber=" + ordernumber + ", locationid=" + locationid + ", dateID=" + dateID + "]";
	}

}
