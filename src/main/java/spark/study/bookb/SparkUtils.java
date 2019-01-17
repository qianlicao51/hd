/**  
 * All rights Reserved, Designed By grq
 * @Title:  SparkUtils.java   
 * @Package spark.study.booka   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: grq  
 * @date:   2019年1月5日 下午4:10:05   
 * @version V1.0 
 * @Copyright: 2019 grq All rights reserved. 
 */
package spark.study.bookb;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author MI
 *
 */
public class SparkUtils {
	static public JavaSparkContext sc;
	static public SparkSession sparkSession;
	 

	static {
		sparkSession = SparkSession.builder().appName("JavaSparkPi").master("local").getOrCreate();
		sc = new JavaSparkContext(sparkSession.sparkContext());
		/**
		 * 使用mybatis 获取资源文件路径
		 */
		 
	}

	public static JavaSparkContext getJavaSparkContext() {
		return sc;
	}
}
