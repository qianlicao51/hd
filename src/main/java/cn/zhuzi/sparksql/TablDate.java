package cn.zhuzi.sparksql;

import java.io.Serializable;

/**
 * @Title: TablDate.java
 * @Package cn.zhuzi.sparksql
 * @Description: TODO(日期的分类)
 * @author 作者 grq
 * @version 创建时间：2018年11月25日 上午1:32:59 日期，年月，年，月，日，周几，第几周，季度，旬、半月
 *          2003-1-1,200301,2003,1,1,3,1,1,1,1 dateID string, theyearmonth
 *          string, theyear string, themonth string, thedate string, theweek
 *          string, theweeks string, thequot string, thetenday string,
 *          thehalfmonth string
 */
public class TablDate implements Serializable {

	private static final long serialVersionUID = 1L;
	private String dateID;
	private String theyearmonth;
	private String theyear;
	private String themonth;
	private String thedate;
	private String theweek;
	private String theweeks;
	private String thequot;
	private String thetenday;
	private String thehalfmonth;

	public String getDateID() {
		return dateID;
	}

	public void setDateID(String dateID) {
		this.dateID = dateID;
	}

	public String getTheyearmonth() {
		return theyearmonth;
	}

	public void setTheyearmonth(String theyearmonth) {
		this.theyearmonth = theyearmonth;
	}

	public String getTheyear() {
		return theyear;
	}

	public void setTheyear(String theyear) {
		this.theyear = theyear;
	}

	public String getThemonth() {
		return themonth;
	}

	public void setThemonth(String themonth) {
		this.themonth = themonth;
	}

	public String getThedate() {
		return thedate;
	}

	public void setThedate(String thedate) {
		this.thedate = thedate;
	}

	public String getTheweek() {
		return theweek;
	}

	public void setTheweek(String theweek) {
		this.theweek = theweek;
	}

	public String getTheweeks() {
		return theweeks;
	}

	public void setTheweeks(String theweeks) {
		this.theweeks = theweeks;
	}

	public String getThequot() {
		return thequot;
	}

	public void setThequot(String thequot) {
		this.thequot = thequot;
	}

	public String getThetenday() {
		return thetenday;
	}

	public void setThetenday(String thetenday) {
		this.thetenday = thetenday;
	}

	public String getThehalfmonth() {
		return thehalfmonth;
	}

	public void setThehalfmonth(String thehalfmonth) {
		this.thehalfmonth = thehalfmonth;
	}

	@Override
	public String toString() {
		return "TablDate [dateID=" + dateID + ", theyearmonth=" + theyearmonth + ", theyear=" + theyear + ", themonth=" + themonth + ", thedate=" + thedate + ", theweek=" + theweek + ", theweeks=" + theweeks + ", thequot=" + thequot + ", thetenday=" + thetenday + ", thehalfmonth=" + thehalfmonth + "]";
	}

}
