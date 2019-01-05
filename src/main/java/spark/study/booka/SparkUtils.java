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
package spark.study.booka;

import java.io.IOException;

import org.apache.ibatis.io.Resources;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author MI
 *
 */
public class SparkUtils {
	static public JavaSparkContext sc;
	static public SparkSession sparkSession;
	public static String base_path = null;

	static {
		sparkSession = SparkSession.builder().appName("JavaSparkPi").master("local").getOrCreate();
		sc = new JavaSparkContext(sparkSession.sparkContext());
		/**
		 * 使用mybatis 获取资源文件路径
		 */
		try {
			base_path = Resources.getResourceAsFile("data/spark").getAbsolutePath();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static JavaSparkContext getJavaSparkContext() {
		return sc;
	}
}
