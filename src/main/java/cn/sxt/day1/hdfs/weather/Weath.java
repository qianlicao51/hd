package cn.sxt.day1.hdfs.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;

import scala.reflect.internal.Trees.This;

public class Weath implements WritableComparable<Weath> {

	private int year;
	private int mon;
	private int day;
	private int wd;

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO 自动生成的方法存根out
		out.writeInt(year);
		out.writeInt(mon);
		out.writeInt(day);
		out.writeInt(wd);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.mon = in.readInt();
		this.day = in.readInt();
		this.wd = in.readInt();
	}

	@Override
	public int compareTo(Weath that) {
		return new CompareToBuilder().append(that.year, this.year).append(that.mon, this.mon).append(that.day, this.day)
				.toComparison();
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMon() {
		return mon;
	}

	public void setMon(int mon) {
		this.mon = mon;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

}
