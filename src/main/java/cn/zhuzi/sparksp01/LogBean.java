package cn.zhuzi.sparksp01;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @Title: LogBean.java
 * @Package cn.zhuzi.sparksp01
 * @Description: TODO(搜狗日志log)
 * @author 作者 grq
 * @version 创建时间：2018年11月24日 下午5:56:38
 *          <p>
 *          日志格式 20111230000005 57375476989eea12893c0c3811607bcf 奇艺高清 1 1
 *          http://www.qiyi.com/
 *          <p>
 */
public class LogBean implements Serializable {

	private static final long serialVersionUID = 1L;
	private String dateStr;
	private String userid;
	private String queryWord;

	public String getDateStr() {
		return dateStr;
	}

	public void setDateStr(String dateStr) {
		this.dateStr = dateStr;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getQueryWord() {
		return queryWord;
	}

	public void setQueryWord(String queryWord) {
		this.queryWord = queryWord;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
