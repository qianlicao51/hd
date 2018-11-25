package cn.zhuzi.sparksql;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @Title: StockDetail.java
 * @Package cn.zhuzi.sparksql
 * @Description: TODO(订单明细)
 * @author 作者 grq
 * @version 创建时间：2018年11月25日 上午1:24:48 ordernumber STRING,rownum int,itemid
 *          STRING,qty INT,price int ,amount int 订单号，行号，货品，数量，单价，金额
 *          BYSL00000893,0,FS527258160501,-1,268,-268
 */
public class StockDetail implements Serializable {

	private static final long serialVersionUID = 1L;
	/**
	 * 订单号
	 */
	private String ordernumber;
	/**
	 * 行号
	 */
	private int rownum;
	/**
	 * 货品
	 */
	private String itemid;
	/**
	 * 数量
	 */
	private int qty;
	/**
	 * 单价
	 */
	private double price;
	/**
	 * 金额
	 */
	private double amount;

	public String getOrdernumber() {
		return ordernumber;
	}

	public void setOrdernumber(String ordernumber) {
		this.ordernumber = ordernumber;
	}

	public int getRownum() {
		return rownum;
	}

	public void setRownum(int rownum) {
		this.rownum = rownum;
	}

	public String getItemid() {
		return itemid;
	}

	public void setItemid(String itemid) {
		this.itemid = itemid;
	}

	public int getQty() {
		return qty;
	}

	public void setQty(int qty) {
		this.qty = qty;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
