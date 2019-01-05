/**  
 * All rights Reserved, Designed By grq
 * @Title:  DateSetDemo.java   
 * @Package spark.study.booka.char14   
 * @Description:    TODO(sql/log.json 数据分析)   
 * @author: grq  
 * @date:   2019年1月5日 下午4:44:29   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.booka.char14;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import spark.study.booka.SparkUtils;

public class DateSetDemo {
	public static void main(String[] args) {

		SparkSession sparkSession = SparkUtils.sparkSession;

		Dataset<Row> logDataset = sparkSession.read().json(SparkUtils.base_path + "/sql/log.json");
		Dataset<Row> userDataset = sparkSession.read().json(SparkUtils.base_path + "/sql/user.json");
		Dataset<Logbean> logDS = logDataset.as(Encoders.bean(Logbean.class));
		top(sparkSession, logDataset, userDataset);
		sparkSession.close();
	}

	/**
	 * 特定时间内访问次数最多
	 * 
	 * @param logDS
	 * @param userDataset
	 */
	private static void top(SparkSession sparkSession, Dataset<Row> logDS, Dataset<Row> userDataset) {
		String startTime = "2016-10-01";
		String endTime = "2016-11-01";
		System.out.println("--统计特定时间内访问最多的Top5--");
		Dataset<Row> filter = logDS.filter("date_format(time, 'yyyy-MM-dd') >= '" + startTime + "' and date_format(time, 'yyyy-MM-dd') <='" + endTime + "' and typed=0 ");
		filter.createOrReplaceTempView("tmp_log");
		userDataset.createOrReplaceTempView("tmp_user");
		sparkSession.sql("select count(logID) as logCount,userID, name from ("//
				+ "select logID ,u.userID, u.name from tmp_log log left join tmp_user u on log.userID=u.userID "//
				+ ") group by userID ,name order by logCount desc limit 5 ").show();
		// |consumed|logID| time|typed|userID| name| registeredTime|userID|

	}
}
